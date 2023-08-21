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
package io.trino.cache;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.iterative.rule.InlineProjections.inlineProjections;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static java.util.Objects.requireNonNull;

public final class CanonicalSubplanExtractor
{
    private CanonicalSubplanExtractor() {}

    /**
     * Extracts a list of {@link CanonicalSubplan} for a given plan.
     */
    public static List<CanonicalSubplan> extractCanonicalSubplans(Metadata metadata, Session session, PlanNode root)
    {
        ImmutableList.Builder<CanonicalSubplan> canonicalSubplans = ImmutableList.builder();
        root.accept(new Visitor(metadata, session, canonicalSubplans), true).ifPresent(canonicalSubplans::add);
        return canonicalSubplans.build();
    }

    private static class Visitor
            extends PlanVisitor<Optional<CanonicalSubplan>, Boolean>
    {
        private final Metadata metadata;
        private final Session session;
        private final ImmutableList.Builder<CanonicalSubplan> canonicalSubplans;

        public Visitor(Metadata metadata, Session session, ImmutableList.Builder<CanonicalSubplan> canonicalSubplans)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.canonicalSubplans = requireNonNull(canonicalSubplans, "canonicalSubplans is null");
        }

        @Override
        protected Optional<CanonicalSubplan> visitPlan(PlanNode node, Boolean recursive)
        {
            if (recursive) {
                node.getSources().forEach(child -> canonicalizeRecursively(child, true));
            }
            return Optional.empty();
        }

        @Override
        public Optional<CanonicalSubplan> visitChooseAlternativeNode(ChooseAlternativeNode node, Boolean recursive)
        {
            // do not canonicalize plans that already contain alternative
            return Optional.empty();
        }

        @Override
        public Optional<CanonicalSubplan> visitAggregation(AggregationNode node, Boolean recursive)
        {
            PlanNode source = node.getSource();

            // always add non-aggregated canonical subplan so that it can be matched against other
            // non-aggregated subqueries
            canonicalizeRecursively(source, recursive);

            if (!(source instanceof ProjectNode || source instanceof FilterNode || source instanceof TableScanNode)) {
                return Optional.empty();
            }

            // only subset of aggregations is supported
            if (!(node.getGroupingSetCount() == 1
                    && node.getPreGroupedSymbols().isEmpty()
                    && node.getStep() == PARTIAL
                    && node.getGroupIdSymbol().isEmpty())) {
                return Optional.empty();
            }

            // only subset of aggregation functions are supported
            boolean allSupportedAggregations = node.getAggregations().values().stream().allMatch(aggregation ->
                    // only symbol arguments are supported (no lambdas yet)
                    aggregation.getArguments().stream().allMatch(argument -> argument instanceof SymbolReference)
                            && !aggregation.isDistinct()
                            && aggregation.getFilter().isEmpty()
                            && aggregation.getOrderingScheme().isEmpty());

            if (!allSupportedAggregations) {
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional = node.getSource().accept(this, false);

            // inline projections used in aggregation
            Optional<Symbol> hashSymbol = node.getHashSymbol();
            if (subplanOptional.isEmpty()
                    && hashSymbol.isPresent()
                    && source instanceof ProjectNode sourceProjection
                    && sourceProjection.getSource() instanceof ProjectNode nestedSourceProjection) {
                subplanOptional = inlineProjections(
                        sourceProjection,
                        nestedSourceProjection,
                        ImmutableSet.<Symbol>builder()
                                .addAll(SymbolsExtractor.extractUnique(sourceProjection.getAssignments().get(hashSymbol.get())))
                                // inline identities
                                .addAll(sourceProjection.getAssignments().getOutputs())
                                .build())
                        .flatMap(projection -> projection.accept(this, false));
            }

            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            // evaluate mapping from subplan symbols to canonical expressions
            CanonicalSubplan subplan = subplanOptional.get();
            Map<Symbol, Expression> canonicalExpressionMap = subplan.getOriginalSymbolMapping().entrySet().stream()
                    .filter(entry -> subplan.getAssignments().containsKey(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getValue, entry -> subplan.getAssignments().get(entry.getKey())));

            ImmutableMap.Builder<CacheColumnId, Expression> assignmentsBuilder = ImmutableMap.builder();
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.<CacheColumnId, Symbol>builder()
                    .putAll(subplan.getOriginalSymbolMapping());

            // canonicalize grouping columns
            ImmutableList.Builder<Expression> groupByExpressions = ImmutableList.builder();
            for (Symbol groupingKey : node.getGroupingKeys()) {
                Expression groupByExpression = requireNonNull(canonicalExpressionMap.get(groupingKey));
                CacheColumnId columnId = requireNonNull(subplan.getOriginalSymbolMapping().inverse().get(groupingKey));
                groupByExpressions.add(groupByExpression);
                assignmentsBuilder.put(columnId, groupByExpression);
            }

            // canonicalize hash column
            Optional<Expression> groupByHash;
            if (node.getHashSymbol().isPresent()) {
                CacheColumnId columnId = requireNonNull(subplan.getOriginalSymbolMapping().inverse().get(hashSymbol.get()));
                groupByHash = Optional.of(requireNonNull(canonicalExpressionMap.get(hashSymbol.get())));
                assignmentsBuilder.put(columnId, groupByHash.get());
            }
            else {
                groupByHash = Optional.empty();
            }

            // canonicalize aggregation functions
            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                FunctionCall canonicalAggregation = new FunctionCall(
                        Optional.empty(),
                        aggregation.getResolvedFunction().toQualifiedName(),
                        Optional.empty(),
                        // represent aggregation mask as filter expression
                        aggregation.getMask().map(mask -> requireNonNull(canonicalExpressionMap.get(mask))),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        aggregation.getArguments().stream()
                                .map(argument -> canonicalExpressionMap.get(new Symbol(((SymbolReference) argument).getName())))
                                .collect(toImmutableList()));
                CacheColumnId columnId = new CacheColumnId(formatExpression(canonicalAggregation));
                assignmentsBuilder.put(columnId, canonicalAggregation);
                symbolMappingBuilder.put(columnId, symbol);
            }

            // validate order of assignments with aggregation output columns
            Map<CacheColumnId, Expression> assignments = assignmentsBuilder.buildOrThrow();
            BiMap<CacheColumnId, Symbol> symbolMapping = symbolMappingBuilder.buildOrThrow();
            verify(ImmutableList.copyOf(assignmentsBuilder.buildOrThrow().keySet())
                            .equals(node.getOutputSymbols().stream()
                                    .map(symbol -> requireNonNull(symbolMapping.inverse().get(symbol)))
                                    .collect(toImmutableList())),
                    "Assignments order doesn't match aggregation output symbols order");

            return Optional.of(new CanonicalSubplan(
                    node,
                    symbolMapping,
                    Optional.of(groupByExpressions.build()),
                    groupByHash,
                    assignments,
                    subplan.getConjuncts(),
                    subplan.getDynamicConjuncts(),
                    subplan.getColumnHandles(),
                    subplan.getTable(),
                    subplan.getTableId(),
                    subplan.isUseConnectorNodePartitioning()));
        }

        @Override
        public Optional<CanonicalSubplan> visitProject(ProjectNode node, Boolean recursive)
        {
            PlanNode source = node.getSource();
            if (!(source instanceof FilterNode || source instanceof TableScanNode)) {
                canonicalizeRecursively(source, recursive);
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional = node.getSource().accept(this, recursive);
            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            CanonicalSubplan subplan = subplanOptional.get();
            // canonicalize projection assignments
            ImmutableMap.Builder<CacheColumnId, Expression> assignments = ImmutableMap.builder();
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.<CacheColumnId, Symbol>builder()
                    .putAll(subplan.getOriginalSymbolMapping());
            for (Map.Entry<Symbol, Expression> assignment : node.getAssignments().entrySet()) {
                Symbol symbol = assignment.getKey();
                if (node.getAssignments().isIdentity(symbol)) {
                    // canonicalize identity assignment
                    checkState(subplan.getOriginalSymbolMapping().inverse().containsKey(symbol));
                    CacheColumnId columnId = subplan.getOriginalSymbolMapping().inverse().get(symbol);
                    assignments.put(columnId, new SymbolReference(columnId.toString()));
                    continue;
                }

                // use formatted canonical expression as column id for non-identity projections
                Expression canonicalExpression = subplan.canonicalSymbolMapper().map(assignment.getValue());
                CacheColumnId columnId = new CacheColumnId(formatExpression(canonicalExpression));
                assignments.put(columnId, canonicalExpression);
                symbolMappingBuilder.put(columnId, symbol);
            }

            return Optional.of(new CanonicalSubplan(
                    node,
                    symbolMappingBuilder.build(),
                    Optional.empty(),
                    Optional.empty(),
                    assignments.buildOrThrow(),
                    subplan.getConjuncts(),
                    subplan.getDynamicConjuncts(),
                    subplan.getColumnHandles(),
                    subplan.getTable(),
                    subplan.getTableId(),
                    subplan.isUseConnectorNodePartitioning()));
        }

        @Override
        public Optional<CanonicalSubplan> visitFilter(FilterNode node, Boolean recursive)
        {
            PlanNode source = node.getSource();
            if (!(source instanceof TableScanNode)) {
                // only scan <- filter <- project or scan <- project plans are supported
                canonicalizeRecursively(source, recursive);
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional = source.accept(this, recursive);
            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            CanonicalSubplan subplan = subplanOptional.get();

            // extract dynamic and static conjuncts
            SymbolMapper canonicalSymbolMapper = subplan.canonicalSymbolMapper();
            ImmutableList.Builder<Expression> conjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> dynamicConjuncts = ImmutableList.builder();
            for (Expression expression : extractConjuncts(node.getPredicate())) {
                if (isDynamicFilter(expression)) {
                    dynamicConjuncts.add(canonicalSymbolMapper.map(expression));
                }
                else {
                    conjuncts.add(canonicalSymbolMapper.map(expression));
                }
            }

            return Optional.of(new CanonicalSubplan(
                    node,
                    subplan.getOriginalSymbolMapping(),
                    Optional.empty(),
                    Optional.empty(),
                    subplan.getAssignments(),
                    conjuncts.build(),
                    dynamicConjuncts.build(),
                    subplan.getColumnHandles(),
                    subplan.getTable(),
                    subplan.getTableId(),
                    subplan.isUseConnectorNodePartitioning()));
        }

        @Override
        public Optional<CanonicalSubplan> visitTableScan(TableScanNode node, Boolean recursive)
        {
            if (node.isUpdateTarget()) {
                // inserts are not supported
                return Optional.empty();
            }

            if (node.isUseConnectorNodePartitioning()) {
                // TODO: add support for node partitioning
                return Optional.empty();
            }

            TableHandle canonicalTableHandle = metadata.getCanonicalTableHandle(session, node.getTable());
            Optional<CacheTableId> tableId = metadata.getCacheTableId(session, canonicalTableHandle)
                    // prepend catalog id
                    .map(id -> new CacheTableId(node.getTable().getCatalogHandle().getId() + ":" + id));
            if (tableId.isEmpty()) {
                return Optional.empty();
            }

            // canonicalize output symbols using column ids
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.builder();
            ImmutableMap.Builder<CacheColumnId, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
            for (Symbol outputSymbol : node.getOutputSymbols()) {
                ColumnHandle columnHandle = node.getAssignments().get(outputSymbol);
                Optional<CacheColumnId> columnId = metadata.getCacheColumnId(
                        session,
                        node.getTable(),
                        columnHandle);
                if (columnId.isEmpty()) {
                    return Optional.empty();
                }
                symbolMappingBuilder.put(columnId.get(), outputSymbol);
                columnHandlesBuilder.put(columnId.get(), columnHandle);
            }
            BiMap<CacheColumnId, Symbol> symbolMapping = symbolMappingBuilder.build();

            // pass-through canonical output symbols
            Map<CacheColumnId, Expression> assignments = symbolMapping.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> new SymbolReference(entry.getKey().toString())));

            return Optional.of(new CanonicalSubplan(
                    node,
                    symbolMapping,
                    Optional.empty(),
                    Optional.empty(),
                    assignments,
                    // No filters in table scan. Relevant pushed predicates are
                    // part of CacheTableId, so such subplans won't be considered
                    // as similar.
                    ImmutableList.of(),
                    // no dynamic filters in table scan
                    ImmutableList.of(),
                    columnHandlesBuilder.buildOrThrow(),
                    canonicalTableHandle,
                    tableId.get(),
                    node.isUseConnectorNodePartitioning()));
        }

        private void canonicalizeRecursively(PlanNode node, boolean recursive)
        {
            if (recursive) {
                node.accept(this, true).ifPresent(canonicalSubplans::add);
            }
        }
    }
}
