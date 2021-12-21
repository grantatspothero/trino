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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.tree.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.union;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 * - Aggregate
 *    - Union
 *       - relation1
 *       - relation2
 *       ..
 * </pre>
 * Into:
 * <pre>
 * - Aggregate
 *    - Union
 *       - Aggregate
 *          - relation1
 *       - Aggregate
 *          - relation2
 *       ..
 * </pre>
 * Only applies to certain types of summable aggregations
 */
public class PushAggregationThroughUnion
        implements Rule<AggregationNode>
{
    private final Metadata metadata;

    public PushAggregationThroughUnion(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    private static final Capture<UnionNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(step().equalTo(AggregationNode.Step.SINGLE))
            .with(source().matching(union().capturedAs(CHILD)));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        if (!parent.isDecomposable(metadata)) {
            return Result.empty();
        }

        UnionNode unionNode = captures.get(CHILD);
        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputs = ImmutableListMultimap.builder();
        Symbol sharedSymbol = context.getSymbolAllocator().newSymbol(unionNode.getSources().get(0).getOutputSymbols().get(0));

        for (PlanNode source : unionNode.getSources()) {
            Symbol newSymbol = context.getSymbolAllocator().newSymbol(unionNode.getSources().get(0).getOutputSymbols().get(0));
            Map<Symbol, AggregationNode.Aggregation> aggregations = new HashMap<>();
            parent.getAggregations().forEach((key, value) -> {
                AggregationNode.Aggregation agg = new AggregationNode.Aggregation(value.getResolvedFunction(), source.getOutputSymbols().stream().map(Symbol::toSymbolReference).collect(Collectors.toList()), value.isDistinct(), value.getFilter(), value.getOrderingScheme(), value.getMask());
                outputsToInputs.put(sharedSymbol, newSymbol);
                aggregations.put(newSymbol, agg);
            });
            builder.add(new AggregationNode(context.getIdAllocator().getNextId(), source, aggregations, parent.getGroupingSets(), parent.getPreGroupedSymbols(), parent.getStep(), parent.getHashSymbol(), parent.getGroupIdSymbol()));
        }

        UnionNode newUnionNode = new UnionNode(unionNode.getId(), builder.build(), outputsToInputs.build(), new ArrayList<>(outputsToInputs.build().keySet()));

        // Generate final aggregate
        // Note the final aggregation might not be using the same agregation function as the pushed down aggregations
        // EG: Sum(A union B) => Sum(sum(A) union
        Symbol finalAggregatedSymbol = parent.getOutputSymbols().get(0);
        Map<Symbol, AggregationNode.Aggregation> finalAggregations = new HashMap<>();
        parent.getAggregations().forEach((key, value) -> {
            AggregationNode.Aggregation agg = new AggregationNode.Aggregation(value.getResolvedFunction(), newUnionNode.getOutputSymbols().stream().map(Symbol::toSymbolReference).collect(Collectors.toList()), value.isDistinct(), value.getFilter(), value.getOrderingScheme(), value.getMask());
            outputsToInputs.put(finalAggregatedSymbol, newUnionNode.getOutputSymbols().get(0));
            finalAggregations.put(finalAggregatedSymbol, agg);
        });
        ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputsFinal = ImmutableListMultimap.builder();
        outputsToInputsFinal.put(finalAggregatedSymbol, newUnionNode.getOutputSymbols().get(0));

        AggregationNode finalNode = new AggregationNode(context.getIdAllocator().getNextId(), newUnionNode, finalAggregations, parent.getGroupingSets(), parent.getPreGroupedSymbols(), parent.getStep(), parent.getHashSymbol(), parent.getGroupIdSymbol());
        return Result.ofPlanNode(finalNode);
    }
}
