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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.DomainTranslator.ExtractionResult;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.cache.CanonicalSubplanExtractor.canonicalSymbolToColumnId;
import static io.trino.cache.CanonicalSubplanExtractor.columnIdToSymbol;
import static io.trino.cache.CanonicalSubplanExtractor.extractCanonicalSubplans;
import static io.trino.plugin.base.cache.CacheUtils.normalizeTupleDomain;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.DynamicFilters.extractSourceSymbol;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.ExpressionUtils.or;
import static io.trino.sql.planner.iterative.rule.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static io.trino.sql.planner.iterative.rule.NormalizeOrExpressionRewriter.normalizeOrExpression;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.pushFilterIntoTableScan;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;

/**
 * Identifies common subqueries and provides adaptation to original query plan. Result of common
 * subquery evaluation is cached with {@link CacheManager}. Therefore, IO and computations are
 * performed only once and are reused within query execution.
 * <p>
 * The general idea is that if there are two subqueries, e.g:
 * subquery1: table_scan(table) <- filter(col1 = 1) <- projection(y := col2 + 1)
 * subquery2: table_scan(table) <- filter(col1 = 2) <- projection(z := col2 * 2)
 * <p>
 * Then such subqueries can be transformed into:
 * subquery1: table_scan(table) <- filter(col1 = 1 OR col1 = 2) <- projection(y := col2 + 1, z := col2 * 2)
 * <- filter(col1 = 1) <- projection(y := y)
 * subquery2: table_scan(table) <- filter(col1 = 1 OR col1 = 2) <- projection(y := col2 + 1, z := col2 * 2)
 * <- filter(col1 = 2) <- projection(z := z)
 * <p>
 * where: table_scan(table) <- filter(col1 = 1 OR col1 = 2) <- projection(y := col2 + 1, z := col2 * 2)
 * is a common subquery for which the results can be cached and evaluated only once.
 */
public final class CommonSubqueriesExtractor
{
    private CommonSubqueriesExtractor() {}

    public static Map<PlanNode, CommonPlanAdaptation> extractCommonSubqueries(
            PlannerContext plannerContext,
            Session session,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            TypeAnalyzer typeAnalyzer,
            StatsProvider statsProvider,
            PlanNode root)
    {
        ImmutableMap.Builder<PlanNode, CommonPlanAdaptation> planAdaptations = ImmutableMap.builder();
        Multimap<CacheTableId, CanonicalSubplan> canonicalSubplans = extractCanonicalSubplans(plannerContext.getMetadata(), session, root).stream()
                // TODO: implement
                .filter(subplan -> subplan.getGroupByColumns().isEmpty())
                .collect(toImmutableListMultimap(CanonicalSubplan::getTableId, identity()));
        // extract common subplan adaptations
        for (CacheTableId tableId : canonicalSubplans.keySet()) {
            Collection<CanonicalSubplan> subplans = canonicalSubplans.get(tableId);
            if (subplans.size() == 1) {
                // skip if subquery has only single occurrence
                continue;
            }

            Expression commonPredicate = extractCommonPredicate(subplans, plannerContext.getMetadata());
            Set<Expression> intersectingConjuncts = extractIntersectingConjuncts(subplans);
            Map<CacheColumnId, Expression> commonProjections = extractCommonProjections(subplans, commonPredicate, intersectingConjuncts);
            Map<CacheColumnId, ColumnHandle> commonColumnHandles = extractCommonColumnHandles(subplans);
            Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);

            PlanSignature planSignature = computePlanSignature(
                    plannerContext,
                    session,
                    symbolAllocator.getTypes(),
                    commonColumnIds,
                    tableId,
                    commonPredicate,
                    commonProjections.keySet().stream()
                            .collect(toImmutableList()));

            // Create adaptation for each subquery
            subplans.forEach(subplan -> planAdaptations.put(
                    subplan.getOriginalPlanNode(),
                    createCommonPlanAdaptation(
                            plannerContext,
                            idAllocator,
                            symbolAllocator,
                            subplan,
                            commonProjections,
                            intersectingConjuncts,
                            commonPredicate,
                            commonColumnHandles,
                            commonColumnIds,
                            typeAnalyzer,
                            session,
                            statsProvider,
                            planSignature)));
        }
        return planAdaptations.buildOrThrow();
    }

    private static Expression extractCommonPredicate(Collection<CanonicalSubplan> subplans, Metadata metadata)
    {
        // When two similar subqueries have different predicates, e.g: subquery1: col = 1, subquery2: col = 2
        // then common subquery must have predicate "col = 1 OR col = 2". Narrowing adaptation predicate is then
        // created for each subquery on top of common subquery.
        return normalizeOrExpression(
                extractCommonPredicates(
                        metadata,
                        or(subplans.stream()
                                .map(subplan -> and(
                                        subplan.getConjuncts()))
                                .collect(toImmutableList()))));
    }

    private static Set<Expression> extractIntersectingConjuncts(Collection<CanonicalSubplan> subplans)
    {
        return subplans.stream()
                .map(subplan -> (Set<Expression>) ImmutableSet.copyOf(subplan.getConjuncts()))
                .reduce(Sets::intersection)
                .map(ImmutableSet::copyOf)
                .orElse(ImmutableSet.of());
    }

    private static Map<CacheColumnId, Expression> extractCommonProjections(
            Collection<CanonicalSubplan> subplans,
            Expression commonPredicate,
            Set<Expression> intersectingConjuncts)
    {
        Set<Expression> commonConjuncts = ImmutableSet.copyOf(extractConjuncts(commonPredicate));
        return Streams.concat(
                        // Extract common projections. Common subquery must contain projections from all subqueries.
                        // Pruning adaptation projection is then created for each subquery on top of common subquery.
                        subplans.stream()
                                .flatMap(subplan -> subplan.getAssignments().entrySet().stream()),
                        // Common subquery must propagate all symbols used in adaptation predicates.
                        subplans.stream()
                                .filter(subplan -> isAdaptationPredicateNeeded(subplan, commonConjuncts))
                                .flatMap(subplan -> subplan.getConjuncts().stream())
                                .filter(conjunct -> !intersectingConjuncts.contains(conjunct))
                                .flatMap(conjunct -> SymbolsExtractor.extractAll(conjunct).stream())
                                .map(symbol -> new SimpleEntry<>(canonicalSymbolToColumnId(symbol), symbol.toSymbolReference())))
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<CacheColumnId, ColumnHandle> extractCommonColumnHandles(Collection<CanonicalSubplan> subplans)
    {
        // Common subquery must select column handles from all subqueries.
        return subplans.stream()
                .flatMap(subplan -> subplan.getColumnHandles().entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<CacheColumnId, Symbol> extractCommonColumnIds(Collection<CanonicalSubplan> subplans)
    {
        Map<CacheColumnId, Symbol> commonColumnIds = new LinkedHashMap<>();
        subplans.stream()
                .flatMap(subplan -> subplan.getOriginalSymbolMapping().entrySet().stream())
                .forEach(entry -> commonColumnIds.putIfAbsent(entry.getKey(), entry.getValue()));
        return commonColumnIds;
    }

    private static PlanSignature computePlanSignature(
            PlannerContext plannerContext,
            Session session,
            TypeProvider typeProvider,
            Map<CacheColumnId, Symbol> commonColumnIds,
            CacheTableId tableId,
            Expression predicate,
            List<CacheColumnId> projections)
    {
        SignatureKey signatureKey = new SignatureKey(tableId.toString());
        TupleDomain<CacheColumnId> signaturePredicate = TupleDomain.all();

        if (!predicate.equals(TRUE_LITERAL)) {
            ExtractionResult extractionResult = DomainTranslator.getExtractionResult(
                    plannerContext,
                    session,
                    predicate,
                    TypeProvider.viewOf(commonColumnIds.entrySet().stream()
                            .collect(toImmutableMap(entry -> columnIdToSymbol(entry.getKey()), entry -> typeProvider.get(entry.getValue())))));
            // Only domains for projected columns can be part of signature predicate
            Set<CacheColumnId> projectionSet = ImmutableSet.copyOf(projections);
            signaturePredicate = extractionResult.getTupleDomain()
                    .transformKeys(CanonicalSubplanExtractor::canonicalSymbolToColumnId)
                    .filter((columnId, domain) -> projectionSet.contains(columnId));
            // Remaining expression and non-projected domains must be part of signature key
            TupleDomain<Symbol> prunedPredicate = extractionResult.getTupleDomain()
                    .filter((symbol, domain) -> !projectionSet.contains(canonicalSymbolToColumnId(symbol)));
            if (!prunedPredicate.isAll() || !extractionResult.getRemainingExpression().equals(TRUE_LITERAL)) {
                signatureKey = new SignatureKey(signatureKey + ":" +
                        formatExpression(combineConjuncts(
                                plannerContext.getMetadata(),
                                new DomainTranslator(plannerContext).toPredicate(session, prunedPredicate),
                                extractionResult.getRemainingExpression())));
            }
        }

        return new PlanSignature(
                signatureKey,
                Optional.empty(),
                projections,
                normalizeTupleDomain(signaturePredicate),
                TupleDomain.all());
    }

    private static CommonPlanAdaptation createCommonPlanAdaptation(
            PlannerContext plannerContext,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            CanonicalSubplan subplan,
            Map<CacheColumnId, Expression> commonProjections,
            Set<Expression> intersectingConjuncts,
            Expression commonPredicate,
            Map<CacheColumnId, ColumnHandle> commonColumnHandles,
            Map<CacheColumnId, Symbol> commonColumnIds,
            TypeAnalyzer typeAnalyzer,
            Session session,
            StatsProvider statsProvider,
            PlanSignature planSignature)
    {
        Map<CacheColumnId, Symbol> subqueryColumnIdMapping = new HashMap<>(subplan.getOriginalSymbolMapping());
        // Create symbol mapping for column ids that were not used in original plan
        commonColumnIds.forEach((key, value) -> subqueryColumnIdMapping.putIfAbsent(key, symbolAllocator.newSymbol(value)));
        SymbolMapper subquerySymbolMapper = new SymbolMapper(symbol -> requireNonNull(subqueryColumnIdMapping.get(canonicalSymbolToColumnId(symbol))));

        SubplanFilter commonSubplanFilter = createSubplanFilter(
                subplan,
                commonPredicate,
                createSubplanTableScan(subplan, commonColumnHandles, idAllocator, subqueryColumnIdMapping),
                subquerySymbolMapper,
                plannerContext,
                idAllocator,
                symbolAllocator,
                typeAnalyzer,
                statsProvider,
                session);
        PlanNode commonSubplan = commonSubplanFilter.subplan();
        commonSubplan = createSubplanProjection(commonSubplan, commonProjections, subqueryColumnIdMapping, subquerySymbolMapper, idAllocator);
        Optional<Expression> adaptationPredicate = createAdaptationPredicate(subplan, ImmutableSet.copyOf(extractConjuncts(commonPredicate)), intersectingConjuncts, subquerySymbolMapper);
        Optional<Assignments> adaptationAssignments = createAdaptationAssignments(commonSubplan, ImmutableList.copyOf(subplan.getAssignments().keySet()), subqueryColumnIdMapping, subquerySymbolMapper);
        Map<CacheColumnId, ColumnHandle> dynamicFilterColumnMapping = createDynamicFilterColumnMapping(subplan);

        return new CommonPlanAdaptation(
                commonSubplan,
                new FilteredTableScan(commonSubplanFilter.tableScan(), commonSubplanFilter.predicate()),
                planSignature,
                dynamicFilterColumnMapping,
                adaptationPredicate,
                adaptationAssignments);
    }

    private static Map<CacheColumnId, ColumnHandle> createDynamicFilterColumnMapping(CanonicalSubplan subplan)
    {
        // Create mapping between dynamic filtering columns and column ids.
        // All dynamic filtering columns are part of planSignature columns
        // because joins are not supported yet.
        return subplan.getDynamicConjuncts().stream()
                .flatMap(conjunct -> extractDynamicFilters(conjunct).getDynamicConjuncts().stream())
                .map(filter -> canonicalSymbolToColumnId(extractSourceSymbol(filter)))
                .distinct()
                .collect(toImmutableMap(identity(), columnId -> subplan.getColumnHandles().get(columnId)));
    }

    private static TableScanNode createSubplanTableScan(
            CanonicalSubplan subplan,
            Map<CacheColumnId, ColumnHandle> columnHandles,
            PlanNodeIdAllocator idAllocator,
            Map<CacheColumnId, Symbol> subqueryColumnIdMapping)
    {
        return new TableScanNode(
                idAllocator.getNextId(),
                // use original table handle as it contains information about
                // split enumeration (e.g. enforced partition or bucket filter) for
                // a given subquery
                subplan.getTable(),
                // Remap column ids into specific subquery symbols
                columnHandles.keySet().stream()
                        .map(subqueryColumnIdMapping::get)
                        .collect(toImmutableList()),
                columnHandles.entrySet().stream()
                        .collect(toImmutableMap(entry -> subqueryColumnIdMapping.get(entry.getKey()), Map.Entry::getValue)),
                // Enforced constraint is not important at this stage of planning
                TupleDomain.all(),
                // Stats are not important at this stage of planning
                Optional.empty(),
                false,
                Optional.of(subplan.isUseConnectorNodePartitioning()));
    }

    private static SubplanFilter createSubplanFilter(
            CanonicalSubplan subplan,
            Expression predicate,
            TableScanNode tableScan,
            SymbolMapper subquerySymbolMapper,
            PlannerContext plannerContext,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            TypeAnalyzer typeAnalyzer,
            StatsProvider statsProvider,
            Session session)
    {
        if (predicate.equals(TRUE_LITERAL) && subplan.getDynamicConjuncts().isEmpty()) {
            return new SubplanFilter(tableScan, Optional.empty(), tableScan);
        }

        Expression predicateWithDynamicFilters =
                // Subquery specific dynamic filters need to be added back to subplan.
                // Actual dynamic filter domains are accounted for in PlanSignature on worker nodes.
                subquerySymbolMapper.map(combineConjuncts(
                        plannerContext.getMetadata(),
                        predicate,
                        and(subplan.getDynamicConjuncts())));
        FilterNode filterNode = new FilterNode(
                idAllocator.getNextId(),
                tableScan,
                predicateWithDynamicFilters);

        // Try to push down predicates to table scan
        Optional<PlanNode> rewritten = pushFilterIntoTableScan(
                filterNode,
                tableScan,
                false,
                session,
                idAllocator,
                symbolAllocator,
                plannerContext,
                typeAnalyzer,
                statsProvider,
                new DomainTranslator(plannerContext))
                .getMainAlternative();

        // If ValuesNode was returned as a result of pushing down predicates we fall back
        // to filterNode to avoid introducing significant changes in plan. Changing node from TableScan to ValuesNode
        // potentially interfere with partitioning - note that this step is executed after planning.
        rewritten = rewritten.filter(not(ValuesNode.class::isInstance));

        if (rewritten.isPresent()) {
            PlanNode node = rewritten.get();
            if (node instanceof FilterNode rewrittenFilterNode) {
                checkState(rewrittenFilterNode.getSource() instanceof TableScanNode, "Expected filter source to be TableScanNode");
                return new SubplanFilter(node, Optional.of(rewrittenFilterNode.getPredicate()), (TableScanNode) rewrittenFilterNode.getSource());
            }
            checkState(node instanceof TableScanNode, "Expected rewritten node to be TableScanNode");
            return new SubplanFilter(node, Optional.empty(), (TableScanNode) node);
        }

        return new SubplanFilter(filterNode, Optional.of(predicateWithDynamicFilters), tableScan);
    }

    private record SubplanFilter(PlanNode subplan, Optional<Expression> predicate, TableScanNode tableScan) {}

    private static PlanNode createSubplanProjection(
            PlanNode subplan,
            Map<CacheColumnId, Expression> projections,
            Map<CacheColumnId, Symbol> subqueryColumnIdMapping,
            SymbolMapper subquerySymbolMapper,
            PlanNodeIdAllocator idAllocator)
    {
        return createSubplanAssignments(subplan, projections, subqueryColumnIdMapping, subquerySymbolMapper)
                .map(assignments -> (PlanNode) new ProjectNode(idAllocator.getNextId(), subplan, assignments))
                .orElse(subplan);
    }

    private static Optional<Assignments> createAdaptationAssignments(
            PlanNode subplan,
            List<CacheColumnId> identities,
            Map<CacheColumnId, Symbol> subqueryColumnIdMapping,
            SymbolMapper subquerySymbolMapper)
    {
        // prune and order common subquery output in order to match original subquery
        return createSubplanAssignments(
                subplan,
                identities.stream().collect(toImmutableMap(identity(), id -> columnIdToSymbol(id).toSymbolReference())),
                subqueryColumnIdMapping,
                subquerySymbolMapper);
    }

    private static Optional<Assignments> createSubplanAssignments(
            PlanNode subplan,
            Map<CacheColumnId, Expression> projections,
            Map<CacheColumnId, Symbol> subqueryColumnIdMapping,
            SymbolMapper subquerySymbolMapper)
    {
        // Remap CacheColumnIds and symbols into specific subquery symbols
        Assignments assignments = Assignments.copyOf(projections.entrySet().stream()
                .collect(toImmutableMap(
                        entry -> subqueryColumnIdMapping.get(entry.getKey()),
                        entry -> subquerySymbolMapper.map(entry.getValue()))));

        // projection is sensitive to output symbols order
        if (subplan.getOutputSymbols().equals(assignments.getOutputs())) {
            return Optional.empty();
        }

        return Optional.of(assignments);
    }

    private static Optional<Expression> createAdaptationPredicate(CanonicalSubplan subplan, Set<Expression> commonConjuncts, Set<Expression> intersectingConjuncts, SymbolMapper subquerySymbolMapper)
    {
        if (!isAdaptationPredicateNeeded(subplan, commonConjuncts)) {
            return Optional.empty();
        }

        return Optional.of(subquerySymbolMapper.map(and(subplan.getConjuncts().stream()
                .filter(conjunct -> !intersectingConjuncts.contains(conjunct))
                .collect(toImmutableList()))));
    }

    private static boolean isAdaptationPredicateNeeded(CanonicalSubplan subplan, Set<Expression> commonConjuncts)
    {
        return !subplan.getConjuncts().isEmpty() && !ImmutableSet.copyOf(subplan.getConjuncts()).equals(commonConjuncts);
    }
}
