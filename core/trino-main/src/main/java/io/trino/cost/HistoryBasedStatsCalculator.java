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
package io.trino.cost;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.jmx.CacheStatsMBean;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cache.CacheMetadata;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.StageInfo;
import io.trino.metadata.Metadata;
import io.trino.server.DynamicFilterService;
import io.trino.spi.QueryId;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.planprinter.PlanNodeStats;
import io.trino.sql.tree.Expression;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.isHistoryBasedStatisticsEnabled;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.execution.StageInfo.getAllStages;
import static io.trino.spi.resourcegroups.QueryType.DATA_DEFINITION;
import static io.trino.spi.resourcegroups.QueryType.DESCRIBE;
import static io.trino.spi.resourcegroups.QueryType.EXPLAIN;
import static io.trino.sql.DynamicFilters.getDescriptor;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.planprinter.PlanNodeStatsSummarizer.aggregateStageStats;
import static java.util.Objects.requireNonNull;

/**
 * This class estimates row count for a given sub-plan based on statistics of already finished queries.
 */
public class HistoryBasedStatsCalculator
        implements StatsCalculator
{
    private static final Logger log = Logger.get(HistoryBasedStatsCalculator.class);
    private static final BiConsumer<PlanNodeId, StatsCachePlanSignature> NO_OP = (planNodeId, statsCachePlanSignature) -> {};
    // row count per sub plan signature
    private final Cache<StatsCachePlanSignature, Long> rowCountCache;
    private final Metadata metadata;
    private final CacheMetadata cacheMetadata;
    private final ComposableStatsCalculator delegate;
    private final StatsNormalizer statsNormalizer;
    // per query StatsCachePlanSignature cache to limit the signature recalculation cost of previously seen plan nodes
    private final Cache<QueryId, Map<PlanNode, Optional<StatsCachePlanSignature>>> signatureCache;
    private final CacheStatsMBean historicalQueryStatsCacheMBean;
    private final CacheStatsMBean signatureCacheMBean;

    @Inject
    public HistoryBasedStatsCalculator(
            Metadata metadata,
            CacheMetadata cacheMetadata,
            ComposableStatsCalculator delegate,
            StatsNormalizer statsNormalizer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.cacheMetadata = requireNonNull(cacheMetadata, "cacheMetadata is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.statsNormalizer = requireNonNull(statsNormalizer, "statsNormalizer is null");
        this.rowCountCache = buildUnsafeCacheWithInvalidationRace(CacheBuilder.newBuilder().maximumSize(10_000).recordStats());
        this.historicalQueryStatsCacheMBean = new CacheStatsMBean(rowCountCache);
        this.signatureCache = buildUnsafeCacheWithInvalidationRace(CacheBuilder.newBuilder().maximumSize(1000).recordStats());
        this.signatureCacheMBean = new CacheStatsMBean(signatureCache);
    }

    // We invalidate only to save memory. The cache entry is never loaded again so there actually is no race.
    @SuppressModernizer
    private static <K, V> Cache<K, V> buildUnsafeCacheWithInvalidationRace(CacheBuilder<? super K, ? super V> cacheBuilder)
    {
        return cacheBuilder.build();
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        PlanNodeStatsEstimate calculatedStats = delegate.calculateStats(node, sourceStats, lookup, session, types, tableStatsProvider);
        Optional<Double> outputRowCount = getOutputRowCount(node, lookup, session);
        return outputRowCount.map(rowCount -> statsNormalizer.normalize(new PlanNodeStatsEstimate(rowCount, calculatedStats.getSymbolStatistics()), types))
                .orElse(calculatedStats);
    }

    @VisibleForTesting
    Optional<Double> getOutputRowCount(PlanNode node, Lookup lookup, Session session)
    {
        if (!isHistoryBasedStatisticsEnabled(session)) {
            return Optional.empty();
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<PlanNode, Optional<StatsCachePlanSignature>> querySignatureCache = signatureCache.getIfPresent(session.getQueryId());
        if (querySignatureCache == null) {
            querySignatureCache = new HashMap<>();
            signatureCache.put(session.getQueryId(), querySignatureCache);
        }
        // get the cached row count if available
        Optional<Double> outputRowCount = extractSignature(node, session, lookup, Optional.of(querySignatureCache), ImmutableSet.of(), Optional.empty(), NO_OP)
                .flatMap(signature -> {
                    Long cachedRowCount = rowCountCache.getIfPresent(signature);
                    if (log.isDebugEnabled()) {
                        if (cachedRowCount == null) {
                            log.debug("missing cache for id: %s, signature: %s", node.getId(), signature);
                        }
                        else {
                            log.debug("using cached row count %s for id: %s, signature: %s", cachedRowCount, node.getId(), signature);
                        }
                    }
                    return Optional.ofNullable(cachedRowCount);
                })
                .map(Long::doubleValue);
        if (log.isDebugEnabled()) {
            log.debug("getOutputRowCount took %s us", (int) Math.ceil((double) stopwatch.elapsed().toNanos() / 1000));
        }
        return outputRowCount;
    }

    public void queryStateChanged(QueryId queryId, QueryState newState)
    {
        // clear signatureCache for queries that finished planning
        if (newState == QueryState.STARTING || newState.isDone()) {
            signatureCache.invalidate(queryId);
        }
    }

    public void queryFinished(QueryInfo finalQueryInfo, PlanNode root, Session session)
    {
        if (!finalQueryInfo.getState().equals(FINISHED)
                || finalQueryInfo.getQueryType()
                .filter(queryType ->
                        queryType.equals(EXPLAIN) ||
                                queryType.equals(DESCRIBE) ||
                                queryType.equals(DATA_DEFINITION))
                .isPresent()) {
            return;
        }

        if (!isHistoryBasedStatisticsEnabled(session)) {
            return;
        }

        if (finalQueryInfo.getOutputStage().isEmpty()) {
            return;
        }
        requireNonNull(root);

        Stopwatch stopwatch = Stopwatch.createStarted();

        // cache row counts per every sub-plan
        List<StageInfo> allStages = getAllStages(finalQueryInfo.getOutputStage());
        Map<PlanNodeId, PlanNodeStats> nodeStats = aggregateStageStats(allStages);
        BiConsumer<PlanNodeId, StatsCachePlanSignature> cacheUpdater = (planNodeId, signature) -> {
            if (signature instanceof TableScan) {
                // do not cache table scans as DF can distort the stats and DF are not part of the key
                return;
            }
            PlanNodeStats planNodeStats = nodeStats.get(planNodeId);
            if (planNodeStats != null) {
                if (log.isDebugEnabled()) {
                    log.debug("caching row count, id: %s, row count: %s, signature: %s", planNodeId, planNodeStats.getPlanNodeOutputPositions(), signature);
                }
                rowCountCache.put(signature, planNodeStats.getPlanNodeOutputPositions());
            }
            else {
                if (log.isDebugEnabled()) {
                    log.debug("missing stats for plan node id: %s, signature: %s", planNodeId, signature);
                }
            }
        };
        extractSignature(
                root,
                session,
                Lookup.noLookup(),
                Optional.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                cacheUpdater);

        Set<DynamicFilterId> nonFilteringDynamicFilters = finalQueryInfo.getQueryStats().getDynamicFiltersStats().getDynamicFilterDomainStats()
                .stream()
                .filter(stats -> stats.getSimplifiedDomain().equals("ALL"))
                .map(DynamicFilterService.DynamicFilterDomainStats::getDynamicFilterId)
                .collect(toImmutableSet());
        if (!nonFilteringDynamicFilters.isEmpty()) {
            // cache signature without dynamic filters that do no filter anything to match plans before DF is added
            extractSignature(
                    root,
                    session,
                    Lookup.noLookup(),
                    Optional.empty(),
                    nonFilteringDynamicFilters,
                    Optional.empty(),
                    cacheUpdater);
        }
        if (log.isDebugEnabled()) {
            log.debug("queryFinished took %s us", (int) Math.ceil((double) stopwatch.elapsed().toNanos() / 1000));
        }
    }

    // extracts signature for the entire sub-plan recursively
    private Optional<StatsCachePlanSignature> extractSignature(
            PlanNode node,
            Session session,
            Lookup lookup,
            Optional<Map<PlanNode, Optional<StatsCachePlanSignature>>> signatureCache,
            Set<DynamicFilterId> nonFilteringDynamicFilters,
            Optional<PlanNodeId> parentProjectionId,
            BiConsumer<PlanNodeId, StatsCachePlanSignature> subPlanSignatureConsumer)
    {
        PlanNode resolved = lookup.resolve(node);
        if (signatureCache.isPresent()) {
            Optional<StatsCachePlanSignature> planSignature = signatureCache.get().get(resolved);
            if (planSignature != null) {
                return planSignature;
            }
        }

        List<StatsCachePlanSignature> sources = new ArrayList<>(resolved.getSources().size());
        Optional<PlanNodeId> projectionId = resolved instanceof ProjectNode ? Optional.of(resolved.getId()) : Optional.empty();
        for (PlanNode source : resolved.getSources()) {
            Optional<StatsCachePlanSignature> sourceSignature = extractSignature(source, session, lookup, signatureCache, nonFilteringDynamicFilters, projectionId, subPlanSignatureConsumer);
            sourceSignature.ifPresent(sources::add);
        }

        Optional<StatsCachePlanSignature> result = Optional.empty();
        if (resolved.getSources().size() == sources.size()) {
            // no unsupported source
            result = extractSignature(resolved, sources, nonFilteringDynamicFilters, session).map(signature -> {
                if (sources.isEmpty() || !signature.equals(sources.get(0))) {
                    // because Project -> Filter -> TableScan nodes are executed using a single ScanFilterAndProjectOperator that has the projection PlanNodeId,
                    // we have to get stats for Filter using parent projection id
                    boolean useParentProjectionId = parentProjectionId.isPresent() && signature instanceof Filter filter && filter.source() instanceof TableScan;
                    PlanNodeId statsId = useParentProjectionId ? parentProjectionId.get() : resolved.getId();

                    subPlanSignatureConsumer.accept(statsId, signature);
                }
                return signature;
            });
        }
        if (signatureCache.isPresent()) {
            signatureCache.get().put(resolved, result);
        }
        return result;
    }

    // extracts shallow signature for a node, given already extracted signatures of sources
    private Optional<StatsCachePlanSignature> extractSignature(
            PlanNode node,
            List<StatsCachePlanSignature> sources,
            Set<DynamicFilterId> nonFilteringDynamicFilters,
            Session session)
    {
        if (node instanceof JoinNode joinNode) {
            return Join.from(joinNode, sources.get(0), sources.get(1), metadata);
        }
        if (node instanceof AggregationNode aggregationNode) {
            return Optional.of(Aggregation.from(aggregationNode, sources.get(0)));
        }
        if (node instanceof ExchangeNode || node instanceof UnionNode) {
            if (node.getSources().size() == 1) {
                // ignore normal exchange
                return Optional.of(sources.get(0));
            }
            return Optional.of(new Union(sources));
        }
        if (node instanceof FilterNode filterNode) {
            return Optional.of(Filter.from(filterNode, sources.get(0), nonFilteringDynamicFilters, metadata));
        }
        if (node instanceof TableScanNode tableScan) {
            return TableScan.from(tableScan, session, cacheMetadata);
        }
        // TODO: cover all the nodes, first easy ones like below
        if (node instanceof ProjectNode || node instanceof MarkDistinctNode || node instanceof AssignUniqueId || node instanceof SortNode) {
            // a node does not change input cardinality, that is the number of output positions is equal to the number of input positions
            verify(sources.size() == 1, "Expected exactly only source for %s but got %s", node, sources);
            return Optional.of(sources.get(0));
        }
        return Optional.empty();
    }

    @Managed
    public void invalidateCache()
    {
        rowCountCache.invalidateAll();
        signatureCache.invalidateAll();
    }

    @Managed
    @Nested
    public CacheStatsMBean getHistoricalQueryStatsCache()
    {
        return historicalQueryStatsCacheMBean;
    }

    @Managed
    @Nested
    public CacheStatsMBean getSessionSignatureCache()
    {
        return signatureCacheMBean;
    }

    interface StatsCachePlanSignature
    {
    }

    record Union(List<StatsCachePlanSignature> sources)
            implements StatsCachePlanSignature
    {
    }

    record Join(JoinNode.Type type, Set<EquiJoinClause> criteria, Optional<Expression> filter, Set<StatsCachePlanSignature> sources)
            implements StatsCachePlanSignature
    {
        public static Optional<StatsCachePlanSignature> from(
                JoinNode node,
                StatsCachePlanSignature left,
                StatsCachePlanSignature right,
                Metadata metadata)
        {
            ImmutableSet.Builder<EquiJoinClause> criteria = ImmutableSet.builder();
            criteria.addAll(node.getCriteria());

            ImmutableSet.Builder<StatsCachePlanSignature> sources = ImmutableSet.builder();
            List<Expression> filters = new ArrayList<>();
            node.getFilter().ifPresent(filters::add);
            // if left or right are joins themselves, merge them with the current join as the join order does not change the output row count
            if (left instanceof Join sourceJoin && sourceJoin.type().equals(node.getType())) {
                sources.addAll(sourceJoin.sources());
                criteria.addAll(sourceJoin.criteria());
                sourceJoin.filter().ifPresent(filters::add);
            }
            else {
                sources.add(left);
            }

            if (right instanceof Join sourceJoin && sourceJoin.type().equals(node.getType())) {
                sources.addAll(sourceJoin.sources());
                criteria.addAll(sourceJoin.criteria());
                sourceJoin.filter().ifPresent(filters::add);
            }
            else {
                sources.add(right);
            }

            Optional<Expression> filter = filters.isEmpty() ? Optional.empty() : Optional.of(combineConjuncts(metadata, filters));
            return Optional.of(new Join(node.getType(), criteria.build(), filter, sources.build()));
        }
    }

    record Aggregation(AggregationNode.Step step, List<Symbol> groupingKeys, Set<Integer> globalGroupingSets, StatsCachePlanSignature source)
            implements StatsCachePlanSignature
    {
        public static StatsCachePlanSignature from(AggregationNode node, StatsCachePlanSignature source)
        {
            if (source instanceof Aggregation sourceAggregation
                    && sourceAggregation.groupingKeys().equals(node.getGroupingKeys())
                    && sourceAggregation.globalGroupingSets().equals(node.getGlobalGroupingSets())) {
                // merge the aggregations as both on the same set of grouping keys
                return new Aggregation(node.getStep().equals(FINAL) ? SINGLE : node.getStep(), node.getGroupingKeys(), node.getGlobalGroupingSets(), sourceAggregation.source());
            }
            return new Aggregation(
                    node.getStep(),
                    node.getGroupingKeys(),
                    node.getGlobalGroupingSets(),
                    source);
        }
    }

    record Filter(Expression predicate, StatsCachePlanSignature source)
            implements StatsCachePlanSignature
    {
        public static StatsCachePlanSignature from(FilterNode filterNode, StatsCachePlanSignature source, Set<DynamicFilterId> nonFilteringDynamicFilters, Metadata metadata)
        {
            // TODO: ultimately the predicate needs to be decoupled from symbols and based on column ids
            Expression predicate = filterNode.getPredicate();
            if (nonFilteringDynamicFilters.isEmpty()) {
                return new Filter(predicate, source);
            }
            // drop dynamic filters in nonFilteringDynamicFilters set
            List<Expression> conjuncts = extractConjuncts(predicate);

            ImmutableList.Builder<Expression> updatedPredicateBuilder = ImmutableList.builder();

            for (Expression conjunct : conjuncts) {
                Optional<DynamicFilters.Descriptor> descriptor = getDescriptor(conjunct);
                if (!(descriptor.isPresent() && nonFilteringDynamicFilters.contains(descriptor.get().getId()))) {
                    updatedPredicateBuilder.add(conjunct);
                }
            }
            List<Expression> updatedPredicate = updatedPredicateBuilder.build();
            if (updatedPredicate.size() == conjuncts.size()) {
                // no DF was dropped
                return new Filter(predicate, source);
            }
            if (updatedPredicate.isEmpty()) {
                // no filter left
                return source;
            }
            return new Filter(combineConjuncts(metadata, updatedPredicate), source);
        }
    }

    record TableScan(String catalogId, CacheTableId tableId, TupleDomain<CacheColumnId> enforcedConstraint)
            implements StatsCachePlanSignature
    {
        public static Optional<StatsCachePlanSignature> from(TableScanNode tableScan, Session session, CacheMetadata cacheMetadata)
        {
            // TODO: when we support different filters, we will need to extract filters pushed down to connectors here
            return cacheMetadata.getCacheTableId(session, tableScan.getTable())
                    .flatMap(id -> {
                        ImmutableMap.Builder<ColumnHandle, CacheColumnId> columnToIdMapBuilder = ImmutableMap.builder();
                        if (tableScan.getEnforcedConstraint().getDomains().isPresent()) {
                            for (ColumnHandle columnHandle : tableScan.getEnforcedConstraint().getDomains().get().keySet()) {
                                Optional<CacheColumnId> cacheColumnId = cacheMetadata.getCacheColumnId(session, tableScan.getTable(), columnHandle);
                                if (cacheColumnId.isEmpty()) {
                                    // cannot extract signature due to missing CacheColumnId
                                    return Optional.empty();
                                }
                                columnToIdMapBuilder.put(columnHandle, cacheColumnId.get());
                            }
                        }
                        Map<ColumnHandle, CacheColumnId> columnToIdMap = columnToIdMapBuilder.buildOrThrow();
                        return Optional.of(new TableScan(
                                tableScan.getTable().getCatalogHandle().getId(),
                                id,
                                tableScan.getEnforcedConstraint().transformKeys(columnToIdMap::get)));
                    });
        }
    }
}
