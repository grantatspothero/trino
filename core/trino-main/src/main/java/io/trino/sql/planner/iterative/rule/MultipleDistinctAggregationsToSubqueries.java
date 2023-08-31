/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.planner.iterative.rule;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.NodeAndMappings;
import io.trino.sql.planner.PlanCopier;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.tree.Expression;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.distinctAggregationsStrategy;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.SPLIT_TO_SUBQUERIES;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/**
 * Transforms plans of the following shape:
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(DISTINCT a0, a1, ...)
 *        F2(DISTINCT b0, b1, ...)
 *        F3(DISTINCT c0, c1, ...)
 *     - X
 * </pre>
 * into
 * <pre>
 * - Join
 *     on left.k = right.k
 *     - Aggregation
 *         GROUP BY (k)
 *         F1(DISTINCT a0, a1, ...)
 *         F2(DISTINCT b0, b1, ...)
 *       - X
 *     - Aggregation
 *         GROUP BY (k)
 *         F3(DISTINCT c0, c1, ...)
 *       - X
 * </pre>
 * <p>
 * This improves plan parallelism and allows {@link SingleDistinctAggregationToGroupBy} to optimize the single input distinct aggregation further.
 * The cost is we calculate X and GROUP BY (k) multiple times, so this rule is only beneficial if the calculations are cheap compared to
 * other distinct aggregation strategies.
 */
public class MultipleDistinctAggregationsToSubqueries
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(
                    Predicates.and(
                            // TODO: we could support non-distinct aggregations if SingleDistinctAggregationToGroupBy supports it
                            SingleDistinctAggregationToGroupBy::allDistinctAggregates,
                            DistinctAggregationToGroupBy::hasMultipleDistincts,
                            // if we have more than one grouping set, we can have duplicated grouping sets and handling this is complex
                            aggregation -> aggregation.getGroupingSetCount() == 1,
                            // hash symbol is added late in the planning, and handling it here would increase complexity
                            aggregation -> aggregation.getHashSymbol().isEmpty()));
    private final Metadata metadata;

    public MultipleDistinctAggregationsToSubqueries(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return distinctAggregationsStrategy(session).equals(SPLIT_TO_SUBQUERIES);
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        if (!isAggregationSourceSupported(aggregationNode.getSource(), context)) {
            return Result.empty();
        }
        // group aggregations by arguments
        Map<Set<Expression>, Map<Symbol, Aggregation>> aggregationsByArguments = new LinkedHashMap<>(aggregationNode.getAggregations().size());
        // sort the aggregation by output symbol to have consistent join layout
        List<Entry<Symbol, Aggregation>> sortedAggregations = aggregationNode.getAggregations().entrySet()
                .stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().getName()))
                .collect(toImmutableList());
        for (Entry<Symbol, Aggregation> entry : sortedAggregations) {
            aggregationsByArguments.compute(ImmutableSet.copyOf(entry.getValue().getArguments()), (key, current) -> {
                if (current == null) {
                    current = new HashMap<>();
                }
                current.put(entry.getKey(), entry.getValue());
                return current;
            });
        }

        PlanNode right = null;
        List<Symbol> rightJoinSymbols = null;
        Assignments.Builder assignments = Assignments.builder();
        List<Map<Symbol, Aggregation>> aggregationsByArgumentsList = ImmutableList.copyOf(aggregationsByArguments.values());
        for (int i = aggregationsByArgumentsList.size() - 1; i > 0; i--) {
            // go from right to left and build the right side of the join
            Map<Symbol, Aggregation> aggregations = aggregationsByArgumentsList.get(i);
            AggregationNode subAggregationNode = buildSubAggregation(aggregationNode, aggregations, assignments, context);

            if (right == null) {
                right = subAggregationNode;
                rightJoinSymbols = subAggregationNode.getGroupingKeys();
            }
            else {
                right = buildJoin(subAggregationNode, subAggregationNode.getGroupingKeys(), right, rightJoinSymbols, context);
            }
        }

        // the first aggregation is the left side of the top join
        AggregationNode left = buildSubAggregation(aggregationNode, aggregationsByArgumentsList.get(0), assignments, context);

        for (int i = 0; i < left.getGroupingKeys().size(); i++) {
            assignments.put(aggregationNode.getGroupingKeys().get(i), left.getGroupingKeys().get(i).toSymbolReference());
        }
        JoinNode topJoin = buildJoin(left, left.getGroupingKeys(), right, rightJoinSymbols, context);
        ProjectNode result = new ProjectNode(aggregationNode.getId(), topJoin, assignments.build());
        return Result.ofPlanNode(result);
    }

    private AggregationNode buildSubAggregation(AggregationNode aggregationNode, Map<Symbol, Aggregation> aggregations, Assignments.Builder assignments, Context context)
    {
        List<Symbol> originalAggregationOutputSymbols = ImmutableList.copyOf(aggregations.keySet());
        // copy the plan so that both plan node ids and symbols are not duplicated between sub aggregations
        NodeAndMappings copied = PlanCopier.copyPlan(
                AggregationNode.builderFrom(aggregationNode).setAggregations(aggregations).build(),
                originalAggregationOutputSymbols,
                metadata,
                context.getSymbolAllocator(),
                context.getIdAllocator(),
                context.getLookup());
        AggregationNode subAggregationNode = (AggregationNode) copied.getNode();
        // add the mapping from the new output symbols to original ones
        for (int i = 0; i < originalAggregationOutputSymbols.size(); i++) {
            assignments.put(originalAggregationOutputSymbols.get(i), copied.getFields().get(i).toSymbolReference());
        }
        return subAggregationNode;
    }

    private JoinNode buildJoin(PlanNode left, List<Symbol> leftJoinSymbols, PlanNode right, List<Symbol> rightJoinSymbols, Context context)
    {
        checkArgument(leftJoinSymbols.size() == rightJoinSymbols.size());
        List<EquiJoinClause> criteria = IntStream.range(0, leftJoinSymbols.size())
                .mapToObj(i -> new EquiJoinClause(leftJoinSymbols.get(i), rightJoinSymbols.get(i)))
                .collect(toImmutableList());

        // TODO: we dont need dynamic filters for this join at all. We could add skipDf field to the JoinNode and make use of it in PredicatePushDown
        return new JoinNode(
                context.getIdAllocator().getNextId(),
                INNER,
                left,
                right,
                criteria,
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false, // since we only work on global aggregation or grouped rows, there are no duplicates, so we don't have to skip it
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    // PlanCopier.copyPlan must support all supported nodes here.
    // Additionally, we should split the table scan only if reading single columns is efficient in the given connector.
    private boolean isAggregationSourceSupported(PlanNode source, Context context)
    {
        if (searchFrom(source, context.getLookup())
                .where(node -> !(node instanceof TableScanNode
                        || node instanceof FilterNode
                        || node instanceof ProjectNode
                        || node instanceof UnionNode))
                .findFirst()
                .isPresent()) {
            return false;
        }

        if (searchFrom(source, context.getLookup())
                .whereIsInstanceOfAny(TableScanNode.class)
                .findAll().stream()
                .anyMatch(tableScanNode -> !metadata.isColumnarTableScan(context.getSession(), ((TableScanNode) tableScanNode).getTable()))) {
            return false;
        }

        return true;
    }
}
