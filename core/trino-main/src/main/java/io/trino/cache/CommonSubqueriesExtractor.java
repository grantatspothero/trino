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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
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
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;

import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.cache.CanonicalSubplanExtractor.canonicalExpressionToColumnId;
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
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
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
        Multimap<SubplanKey, CanonicalSubplan> canonicalSubplans = extractCanonicalSubplans(plannerContext.getMetadata(), session, root).stream()
                .map(subplan -> new SimpleEntry<>(toSubplanKey(subplan), subplan))
                .sorted(Comparator.comparing(entry -> entry.getKey().getPriority()))
                .collect(toImmutableListMultimap(SimpleEntry::getKey, SimpleEntry::getValue));
        // extract common subplan adaptations
        Set<PlanNodeId> processedSubplans = new HashSet<>();
        for (SubplanKey key : canonicalSubplans.keySet()) {
            List<CanonicalSubplan> subplans = canonicalSubplans.get(key).stream()
                    // skip subqueries for which common subplan was already extracted
                    .filter(subplan -> !processedSubplans.contains(subplan.getTableScanId()))
                    .collect(toImmutableList());

            if (subplans.size() <= 1) {
                // skip if subquery has only single occurrence
                continue;
            }

            subplans.forEach(subplan -> processedSubplans.add(subplan.getTableScanId()));

            Expression commonPredicate = extractCommonPredicate(subplans, plannerContext.getMetadata());
            Set<Expression> intersectingConjuncts = extractIntersectingConjuncts(subplans);
            Optional<CacheColumnId> groupByHash = extractGroupByHash(subplans);
            Map<CacheColumnId, Expression> commonProjections = extractCommonProjections(subplans, commonPredicate, intersectingConjuncts, groupByHash, key.groupByColumns());
            Map<CacheColumnId, ColumnHandle> commonColumnHandles = extractCommonColumnHandles(subplans);
            Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);

            PlanSignature planSignature = computePlanSignature(
                    plannerContext,
                    session,
                    symbolAllocator.getTypes(),
                    commonColumnIds,
                    key.tableId(),
                    commonPredicate,
                    commonProjections.keySet().stream()
                            .collect(toImmutableList()),
                    key.groupByColumns());

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
                            groupByHash,
                            typeAnalyzer,
                            session,
                            statsProvider,
                            planSignature)));
        }
        return planAdaptations.buildOrThrow();
    }

    private static SubplanKey toSubplanKey(CanonicalSubplan subplan)
    {
        return toSubplanKey(subplan.getTableId(), subplan.getGroupByColumns(), subplan.getConjuncts());
    }

    @VisibleForTesting
    static SubplanKey toSubplanKey(CacheTableId tableId, Optional<Set<CacheColumnId>> groupByColumns, List<Expression> conjuncts)
    {
        if (groupByColumns.isEmpty()) {
            return new SubplanKey(tableId, Optional.empty(), ImmutableSet.of());
        }

        Set<Symbol> groupBySymbols = groupByColumns.get().stream()
                .map(CanonicalSubplanExtractor::columnIdToSymbol)
                .collect(toImmutableSet());

        // extract conjuncts that can't be pulled though group by columns
        Set<Expression> nonPullableConjuncts = conjuncts.stream()
                .filter(expression -> !groupBySymbols.containsAll(SymbolsExtractor.extractAll(expression)))
                .collect(toImmutableSet());

        return new SubplanKey(tableId, groupByColumns, nonPullableConjuncts);
    }

    record SubplanKey(
            CacheTableId tableId,
            Optional<Set<CacheColumnId>> groupByColumns,
            // conjuncts that cannot be pulled up though aggregation
            Set<Expression> nonPullableConjuncts)
    {
        public int getPriority()
        {
            return groupByColumns.isPresent() ? 0 : 1;
        }
    }

    private static Expression extractCommonPredicate(List<CanonicalSubplan> subplans, Metadata metadata)
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

    private static Set<Expression> extractIntersectingConjuncts(List<CanonicalSubplan> subplans)
    {
        return subplans.stream()
                .map(subplan -> (Set<Expression>) ImmutableSet.copyOf(subplan.getConjuncts()))
                .reduce(Sets::intersection)
                .map(ImmutableSet::copyOf)
                .orElse(ImmutableSet.of());
    }

    private static Optional<CacheColumnId> extractGroupByHash(List<CanonicalSubplan> subplans)
    {
        // Only single hash expression from first subplan is cached as
        // hash combination order (for group by columns) is variable and
        // depends on order of group by columns.
        return subplans.stream()
                .filter(subplan -> subplan.getGroupByHash().isPresent())
                .map(subplan -> subplan.getGroupByHash().get())
                .findFirst();
    }

    private static Map<CacheColumnId, Expression> extractCommonProjections(
            List<CanonicalSubplan> subplans,
            Expression commonPredicate,
            Set<Expression> intersectingConjuncts,
            Optional<CacheColumnId> groupByHash,
            Optional<Set<CacheColumnId>> groupByColumns)
    {
        Set<CacheColumnId> excludedGroupByHashes = subplans.stream()
                .map(CanonicalSubplan::getGroupByHash)
                .filter(Optional::isPresent)
                // Only group by hash expression from first subplan is cached.
                .filter(id -> !groupByHash.equals(id))
                .map(Optional::get)
                .collect(toImmutableSet());
        // Extract common projections. Common (cached) subquery must contain projections from all subqueries.
        // Pruning adaptation projection is then created for each subquery on top of common subplan.
        Map<CacheColumnId, Expression> commonProjections = subplans.stream()
                .flatMap(subplan -> subplan.getAssignments().entrySet().stream())
                .filter(entry -> !excludedGroupByHashes.contains(entry.getKey()))
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        // Common subquery must propagate all symbols used in adaptation predicates.
        Map<CacheColumnId, Expression> propagatedSymbols = subplans.stream()
                .filter(subplan -> isAdaptationPredicateNeeded(subplan, commonPredicate))
                .flatMap(subplan -> subplan.getConjuncts().stream())
                .filter(conjunct -> !intersectingConjuncts.contains(conjunct))
                .flatMap(conjunct -> SymbolsExtractor.extractAll(conjunct).stream())
                .distinct()
                .collect(toImmutableMap(CanonicalSubplanExtractor::canonicalSymbolToColumnId, Symbol::toSymbolReference));
        groupByColumns.ifPresent(columns -> checkState(columns.containsAll(propagatedSymbols.keySet()), "group by columns don't contain all propagated symbols"));
        return Streams.concat(commonProjections.entrySet().stream(), propagatedSymbols.entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<CacheColumnId, ColumnHandle> extractCommonColumnHandles(List<CanonicalSubplan> subplans)
    {
        // Common subquery must select column handles from all subqueries.
        return subplans.stream()
                .flatMap(subplan -> subplan.getColumnHandles().entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<CacheColumnId, Symbol> extractCommonColumnIds(List<CanonicalSubplan> subplans)
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
            List<CacheColumnId> projections,
            Optional<Set<CacheColumnId>> groupByColumns)
    {
        SignatureKey signatureKey = new SignatureKey(tableId.toString());
        // Order group by columns according to projected columns
        Optional<List<CacheColumnId>> orderedGroupByColumns = groupByColumns.map(
                columns -> projections.stream()
                        .filter(columns::contains)
                        .collect(toImmutableList()));
        checkState(groupByColumns.isEmpty() || groupByColumns.get().containsAll(orderedGroupByColumns.get()));

        if (predicate.equals(TRUE_LITERAL)) {
            return new PlanSignature(
                    signatureKey,
                    orderedGroupByColumns,
                    projections,
                    TupleDomain.all(),
                    TupleDomain.all());
        }

        ExtractionResult extractionResult = DomainTranslator.getExtractionResult(
                plannerContext,
                session,
                predicate,
                TypeProvider.viewOf(commonColumnIds.entrySet().stream()
                        .collect(toImmutableMap(entry -> columnIdToSymbol(entry.getKey()), entry -> typeProvider.get(entry.getValue())))));
        // Only domains for projected columns can be part of signature predicate
        Set<CacheColumnId> projectionSet = ImmutableSet.copyOf(projections);
        TupleDomain<CacheColumnId> signaturePredicate = extractionResult.getTupleDomain()
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

        return new PlanSignature(
                signatureKey,
                orderedGroupByColumns,
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
            Optional<CacheColumnId> groupByHash,
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
        if (subplan.getGroupByColumns().isPresent()) {
            commonSubplan = createSubplanAggregation(
                    commonSubplan,
                    commonProjections,
                    subplan.getGroupByColumns().get(),
                    groupByHash,
                    subqueryColumnIdMapping,
                    idAllocator,
                    plannerContext.getMetadata());
        }
        else {
            commonSubplan = createSubplanProjection(commonSubplan, commonProjections, subqueryColumnIdMapping, idAllocator);
        }
        Optional<Expression> adaptationPredicate = createAdaptationPredicate(subplan, commonPredicate, intersectingConjuncts, subquerySymbolMapper);
        Optional<Assignments> adaptationAssignments = createAdaptationAssignments(commonSubplan, subplan, subqueryColumnIdMapping, subquerySymbolMapper);
        Map<CacheColumnId, ColumnHandle> dynamicFilterColumnMapping = createDynamicFilterColumnMapping(subplan);

        return new CommonPlanAdaptation(
                commonSubplan,
                new FilteredTableScan(commonSubplanFilter.tableScan(), commonSubplanFilter.predicate()),
                planSignature,
                dynamicFilterColumnMapping,
                adaptationPredicate,
                adaptationAssignments);
    }

    private static PlanNode createSubplanAggregation(
            PlanNode subplan,
            Map<CacheColumnId, Expression> projections,
            Set<CacheColumnId> groupByColumns,
            Optional<CacheColumnId> groupByHash,
            Map<CacheColumnId, Symbol> columnIdMapping,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata)
    {
        Map<CacheColumnId, Expression> preAggregateProjections = new LinkedHashMap<>();
        ImmutableList.Builder<Symbol> groupByColumnSymbols = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();

        for (Map.Entry<CacheColumnId, Expression> entry : projections.entrySet()) {
            CacheColumnId id = entry.getKey();
            if (groupByColumns.contains(entry.getKey())) {
                // Evaluate group by column expression directly
                preAggregateProjections.put(id, entry.getValue());
                groupByColumnSymbols.add(columnIdMapping.get(id));
            }
            // A separate projection will be created for evaluating group by hash expression
            else if (!groupByHash.equals(Optional.of(id))) {
                FunctionCall aggregationCall = (FunctionCall) entry.getValue();

                // Evaluate filter expression
                Optional<Symbol> mask = aggregationCall.getFilter().map(filter -> registerProjectionExpression(preAggregateProjections, columnIdMapping, filter));

                // Evaluate arguments
                ResolvedFunction resolvedFunction = metadata.decodeFunction(aggregationCall.getName());
                List<Expression> arguments = aggregationCall.getArguments().stream()
                        .map(argument -> registerProjectionExpression(preAggregateProjections, columnIdMapping, argument).toSymbolReference())
                        .collect(toImmutableList());

                // Re-create aggregation using subquery specific symbols
                aggregations.put(
                        columnIdMapping.get(id),
                        new Aggregation(
                                resolvedFunction,
                                arguments,
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                mask));
            }
        }

        subplan = createSubplanProjection(subplan, preAggregateProjections, columnIdMapping, idAllocator);
        // Skip identity projection since order of symbols for pre-aggregate projection is irrelevant
        if (isOrderInsensitiveIdentityProjection(subplan)) {
            subplan = getOnlyElement(subplan.getSources());
        }

        Optional<Symbol> groupByHashSymbol = Optional.empty();
        if (groupByHash.isPresent()) {
            // Append a hash generating projection
            CacheColumnId hashId = groupByHash.get();
            groupByHashSymbol = Optional.of(columnIdMapping.get(hashId));
            subplan = createSubplanProjection(
                    subplan,
                    ImmutableMap.<CacheColumnId, Expression>builder()
                            // Identity passthrough for all columns apart hash column
                            .putAll(preAggregateProjections.keySet().stream()
                                    .collect(toImmutableMap(identity(), id -> columnIdToSymbol(id).toSymbolReference())))
                            .put(hashId, projections.get(hashId))
                            .buildOrThrow(),
                    columnIdMapping,
                    idAllocator);
        }

        AggregationNode aggregation = new AggregationNode(
                idAllocator.getNextId(),
                subplan,
                aggregations.buildOrThrow(),
                singleGroupingSet(groupByColumnSymbols.build()),
                ImmutableList.of(),
                PARTIAL,
                groupByHashSymbol,
                Optional.empty());
        List<Symbol> expectedSymbols = projections.keySet().stream()
                .map(columnIdMapping::get)
                .collect(toImmutableList());
        checkState(aggregation.getOutputSymbols().equals(expectedSymbols), "Aggregation symbols (%s) don't match expected symbols (%s)", aggregation.getOutputSymbols(), expectedSymbols);
        return aggregation;
    }

    private static boolean isOrderInsensitiveIdentityProjection(PlanNode planNode)
    {
        return planNode instanceof ProjectNode projection
                && ImmutableSet.copyOf(projection.getOutputSymbols()).equals(ImmutableSet.copyOf(projection.getSource().getOutputSymbols()));
    }

    private static Symbol registerProjectionExpression(Map<CacheColumnId, Expression> projections, Map<CacheColumnId, Symbol> columnIdMapping, Expression projection)
    {
        CacheColumnId id = canonicalExpressionToColumnId(projection);
        projections.putIfAbsent(id, projection);
        return requireNonNull(columnIdMapping.get(id));
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
            PlanNodeIdAllocator idAllocator)
    {
        SymbolMapper symbolMapper = new SymbolMapper(symbol -> requireNonNull(subqueryColumnIdMapping.get(canonicalSymbolToColumnId(symbol))));
        return createSubplanAssignments(subplan, projections, subqueryColumnIdMapping, symbolMapper)
                .map(assignments -> (PlanNode) new ProjectNode(idAllocator.getNextId(), subplan, assignments))
                .orElse(subplan);
    }

    private static Optional<Assignments> createAdaptationAssignments(
            PlanNode subplan,
            CanonicalSubplan canonicalSubplan,
            Map<CacheColumnId, Symbol> subqueryColumnIdMapping,
            SymbolMapper subquerySymbolMapper)
    {
        // Prune and order common subquery output in order to match original subquery.
        ImmutableMap.Builder<CacheColumnId, Expression> projections = ImmutableMap.builder();
        for (Map.Entry<CacheColumnId, Expression> assignment : canonicalSubplan.getAssignments().entrySet()) {
            CacheColumnId id = assignment.getKey();
            if (!subplan.getOutputSymbols().contains(requireNonNull(subqueryColumnIdMapping.get(id)))) {
                // Add hash computation back if needed.
                checkState(canonicalSubplan.getGroupByHash().equals(Optional.of(id)), "No symbol for column id: %s", id);
                projections.put(id, assignment.getValue());
            }
            else {
                projections.put(id, columnIdToSymbol(id).toSymbolReference());
            }
        }
        return createSubplanAssignments(
                subplan,
                projections.buildOrThrow(),
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

    private static Optional<Expression> createAdaptationPredicate(CanonicalSubplan subplan, Expression commonPredicate, Set<Expression> intersectingConjuncts, SymbolMapper subquerySymbolMapper)
    {
        if (!isAdaptationPredicateNeeded(subplan, commonPredicate)) {
            return Optional.empty();
        }

        return Optional.of(subquerySymbolMapper.map(and(subplan.getConjuncts().stream()
                // use only conjuncts that are not enforced by common predicate
                .filter(conjunct -> !intersectingConjuncts.contains(conjunct))
                .collect(toImmutableList()))));
    }

    private static boolean isAdaptationPredicateNeeded(CanonicalSubplan subplan, Expression commonPredicate)
    {
        Set<Expression> commonConjuncts = ImmutableSet.copyOf(extractConjuncts(commonPredicate));
        return !subplan.getConjuncts().isEmpty() && !ImmutableSet.copyOf(subplan.getConjuncts()).equals(commonConjuncts);
    }
}
