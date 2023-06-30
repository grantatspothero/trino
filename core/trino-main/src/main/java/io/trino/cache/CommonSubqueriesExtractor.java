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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.DomainTranslator.ExtractionResult;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.cache.CanonicalSubplanExtractor.extractCanonicalSubplans;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.ExpressionUtils.or;
import static io.trino.sql.planner.iterative.rule.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static io.trino.sql.planner.iterative.rule.NormalizeOrExpressionRewriter.normalizeOrExpression;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.function.Function.identity;

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
            PlanNode root)
    {
        ImmutableMap.Builder<PlanNode, CommonPlanAdaptation> planAdaptations = ImmutableMap.builder();
        Multimap<CacheTableId, CanonicalSubplan> canonicalSubplans = extractCanonicalSubplans(plannerContext.getMetadata(), session, root).stream()
                .collect(toImmutableListMultimap(CanonicalSubplan::getTableId, identity()));
        // extract common subplan adaptations
        for (CacheTableId tableId : canonicalSubplans.keySet()) {
            Collection<CanonicalSubplan> subplans = canonicalSubplans.get(tableId);
            if (subplans.size() == 1) {
                // skip if subquery has only single occurrence
                continue;
            }

            // When two similar subqueries have different predicates, e.g: subquery1: col = 1, subquery2: col = 2
            // then common subquery must have predicate "col = 1 OR col = 2". Narrowing adaptation predicate is then
            // created for each subquery on top of common subquery.
            Expression commonPredicate =
                    normalizeOrExpression(
                            extractCommonPredicates(
                                    plannerContext.getMetadata(),
                                    or(subplans.stream()
                                            .map(subplan -> and(
                                                    subplan.getConjuncts()))
                                            .collect(toImmutableList()))));

            Set<Expression> commonConjuncts = ImmutableSet.copyOf(extractConjuncts(commonPredicate));
            Map<CacheColumnId, Expression> commonProjections = Streams.concat(
                            // Extract common projections. Common subquery must contain projections from all subqueries.
                            // Pruning adaptation projection is then created for each subquery on top of common subquery.
                            subplans.stream()
                                    .flatMap(subplan -> subplan.getAssignments().entrySet().stream()),
                            // Common subquery must propagate all symbols used in adaptation predicates.
                            subplans.stream()
                                    .filter(subplan -> isAdaptationPredicateNeeded(subplan, commonConjuncts))
                                    .flatMap(subplan -> SymbolsExtractor.extractAll(and(subplan.getConjuncts())).stream())
                                    .map(symbol -> new SimpleEntry<>(canonicalSymbolToColumnId(symbol), symbol.toSymbolReference())))
                    .distinct()
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            // Common subquery must select column handles from all subqueries.
            Map<CacheColumnId, ColumnHandle> commonColumnHandles = subplans.stream()
                    .flatMap(subplan -> subplan.getColumnHandles().entrySet().stream())
                    .distinct()
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            // Extract CacheColumnIds used in common subquery
            Map<CacheColumnId, Type> commonColumnIds = new LinkedHashMap<>();
            subplans.stream()
                    .flatMap(subplan -> subplan.getOriginalSymbolMapping().entrySet().stream())
                    .forEach(entry -> commonColumnIds.putIfAbsent(entry.getKey(), symbolAllocator.getTypes().get(entry.getValue())));

            PlanSignature planSignature = computePlanSignature(
                    plannerContext,
                    session,
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
                            commonPredicate,
                            commonColumnHandles,
                            commonColumnIds,
                            planSignature)));
        }
        return planAdaptations.buildOrThrow();
    }

    private static PlanSignature computePlanSignature(
            PlannerContext plannerContext,
            Session session,
            Map<CacheColumnId, Type> commonColumnIds,
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
                            .collect(toImmutableMap(entry -> columnIdToSymbol(entry.getKey()), Map.Entry::getValue))));
            // Only domains for projected columns can be part of signature
            Set<CacheColumnId> projectionSet = ImmutableSet.copyOf(projections);
            signaturePredicate = extractionResult.getTupleDomain()
                    .transformKeys(CommonSubqueriesExtractor::canonicalSymbolToColumnId)
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

        return new PlanSignature(signatureKey, Optional.empty(), projections, signaturePredicate);
    }

    private static CommonPlanAdaptation createCommonPlanAdaptation(
            PlannerContext plannerContext,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            CanonicalSubplan subplan,
            Map<CacheColumnId, Expression> commonProjections,
            Expression commonPredicate,
            Map<CacheColumnId, ColumnHandle> commonColumnHandles,
            Map<CacheColumnId, Type> commonColumnIds,
            PlanSignature planSignature)
    {
        Map<CacheColumnId, Symbol> subqueryColumnIdMapping = new HashMap<>(subplan.getOriginalSymbolMapping());
        // Create symbol mapping for column ids that were not used in original plan
        commonColumnIds.forEach((key, value) -> subqueryColumnIdMapping.putIfAbsent(key, symbolAllocator.newSymbol(key.toString(), value)));
        SymbolMapper subquerySymbolMapper = new SymbolMapper(subqueryColumnIdMapping.entrySet().stream()
                .collect(toImmutableMap(entry -> columnIdToSymbol(entry.getKey()), Map.Entry::getValue))::get);

        PlanNode commonSubplan = new TableScanNode(
                subplan.getTableScanId(),
                subplan.getTable(),
                // Remap column ids into specific subquery symbols
                commonColumnHandles.keySet().stream()
                        .map(subqueryColumnIdMapping::get)
                        .collect(toImmutableList()),
                commonColumnHandles.entrySet().stream()
                        .collect(toImmutableMap(entry -> subqueryColumnIdMapping.get(entry.getKey()), Map.Entry::getValue)),
                // Enforced constraint is not important at this stage of planning
                TupleDomain.all(),
                // Stats are not important at this stage of planning
                Optional.empty(),
                false,
                Optional.of(subplan.isUseConnectorNodePartitioning()));

        if (!commonPredicate.equals(TRUE_LITERAL) || !subplan.getDynamicConjuncts().isEmpty()) {
            commonSubplan = new FilterNode(
                    idAllocator.getNextId(),
                    commonSubplan,
                    // Subquery specific dynamic filters need to be added back to common subplan.
                    // Actual dynamic filter domains are accounted for in PlanSignature on worker nodes.
                    subquerySymbolMapper.map(combineConjuncts(
                            plannerContext.getMetadata(),
                            commonPredicate,
                            and(subplan.getDynamicConjuncts()))));
        }

        if (!commonProjections.keySet().equals(commonColumnHandles.keySet())) {
            commonSubplan = new ProjectNode(
                    idAllocator.getNextId(),
                    commonSubplan,
                    // Remap CacheColumnIds and symbols into specific subquery symbols
                    Assignments.copyOf(commonProjections.entrySet().stream()
                            .collect(toImmutableMap(
                                    entry -> subqueryColumnIdMapping.get(entry.getKey()),
                                    entry -> subquerySymbolMapper.map(entry.getValue())))));
        }

        Optional<Expression> adaptationPredicate = Optional.empty();
        if (isAdaptationPredicateNeeded(subplan, ImmutableSet.copyOf(extractConjuncts(commonPredicate)))) {
            adaptationPredicate = Optional.of(subquerySymbolMapper.map(and(subplan.getConjuncts())));
        }

        Optional<Assignments> adaptationAssignments = Optional.empty();
        if (!commonProjections.keySet().equals(subplan.getAssignments().keySet())) {
            adaptationAssignments = Optional.of(Assignments.copyOf(subplan.getAssignments().entrySet().stream()
                    .collect(toImmutableMap(
                            entry -> subqueryColumnIdMapping.get(entry.getKey()),
                            // expression is already evaluated in common subquery, therefore symbol just needs to be passed through
                            entry -> subqueryColumnIdMapping.get(entry.getKey()).toSymbolReference()))));
        }

        return new CommonPlanAdaptation(commonSubplan, planSignature, adaptationPredicate, adaptationAssignments);
    }

    private static boolean isAdaptationPredicateNeeded(CanonicalSubplan subplan, Set<Expression> commonConjuncts)
    {
        return !subplan.getConjuncts().isEmpty() && !ImmutableSet.copyOf(subplan.getConjuncts()).equals(commonConjuncts);
    }

    private static CacheColumnId canonicalSymbolToColumnId(Symbol symbol)
    {
        return new CacheColumnId(symbol.getName());
    }

    private static Symbol columnIdToSymbol(CacheColumnId columnId)
    {
        return new Symbol(columnId.toString());
    }
}
