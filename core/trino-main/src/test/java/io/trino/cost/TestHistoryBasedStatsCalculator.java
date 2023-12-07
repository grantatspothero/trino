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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logging;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cache.CacheMetadata;
import io.trino.client.NodeVersion;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStats;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.StageState;
import io.trino.execution.StageStats;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.operator.PipelineStats;
import io.trino.operator.RetryPolicy;
import io.trino.operator.TaskStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.server.DynamicFilterService;
import io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import io.trino.spi.QueryId;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.eventlistener.StageGcStatistics;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.sql.DynamicFilters;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTransactionHandle;
import io.trino.transaction.TransactionId;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.log.Level.DEBUG;
import static io.airlift.log.Level.INFO;
import static io.trino.SystemSessionProperties.HISTORY_BASED_STATISTICS_ENABLED;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.function.FunctionId.toFunctionId;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.resourcegroups.QueryType.DATA_DEFINITION;
import static io.trino.spi.resourcegroups.QueryType.DESCRIBE;
import static io.trino.spi.resourcegroups.QueryType.EXPLAIN;
import static io.trino.spi.resourcegroups.QueryType.SELECT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestHistoryBasedStatsCalculator
{
    private static final ConnectorCacheMetadata CONNECTOR_CACHE_METADATA = new ConnectorCacheMetadata()
    {
        @Override
        public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle)
        {
            return Optional.of(new CacheTableId(tableHandle.toString()));
        }

        @Override
        public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
        {
            return Optional.of(new CacheColumnId(columnHandle.toString()));
        }

        @Override
        public ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle handle)
        {
            return handle;
        }
    };
    private static final CacheMetadata CACHE_METADATA = new CacheMetadata(catalogHandle -> Optional.of(CONNECTOR_CACHE_METADATA));
    private static final AbstractMockMetadata METADATA = new AbstractMockMetadata()
    {
        @Override
        public void cleanupQuery(Session session)
        {
        }

        @Override
        public Optional<CatalogHandle> getCatalogHandle(Session session, String catalogName)
        {
            return Optional.of(TEST_CATALOG_HANDLE);
        }

        @Override
        public ResolvedFunction resolveBuiltinFunction(String name, List<TypeSignatureProvider> parameterTypes)
        {
            if (name.equals(DynamicFilters.Function.NAME)) {
                BoundSignature boundSignature = new BoundSignature(
                        new CatalogSchemaFunctionName(GlobalSystemConnector.NAME, BUILTIN_SCHEMA, DynamicFilters.Function.NAME),
                        UNKNOWN,
                        ImmutableList.of());
                return new ResolvedFunction(
                        boundSignature,
                        GlobalSystemConnector.CATALOG_HANDLE,
                        toFunctionId(DynamicFilters.Function.NAME, boundSignature.toSignature()),
                        SCALAR,
                        true,
                        new FunctionNullability(false, ImmutableList.of()),
                        ImmutableMap.of(),
                        ImmutableSet.of());
            }
            return super.resolveBuiltinFunction(name, parameterTypes);
        }
    };

    @BeforeAll
    public void enableLogging()
    {
        Logging.initialize().setLevel(HistoryBasedStatsCalculator.class.getName(), DEBUG);
    }

    @AfterAll
    public void disableLogging()
    {
        Logging.initialize().setLevel(HistoryBasedStatsCalculator.class.getName(), INFO);
    }

    @Test
    public void testFilterCachedButNotTableScan()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        FilterNode node = new FilterNode(new PlanNodeId("filter"), tableScan("tableScan", "table"), expression("a=b"));

        // both aren't cached
        assertThat(calculator.getOutputRowCount(node, Lookup.noLookup(), session)).isEmpty();
        assertThat(calculator.getOutputRowCount(node.getSource(), Lookup.noLookup(), session)).isEmpty();

        calculator.queryFinished(queryInfo(ImmutableMap.of(node.getId(), 777)), node, session);

        // filter cached after queryFinished
        assertThat(calculator.getOutputRowCount(node, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(777d);

        // table scan still isn't cached
        assertThat(calculator.getOutputRowCount(node.getSource(), Lookup.noLookup(), session)).isEmpty();
    }

    @Test
    public void testScanFilterProjectCachedUsingProjectId()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        ProjectNode node = new ProjectNode(
                new PlanNodeId("project"),
                new FilterNode(
                        new PlanNodeId("filter"),
                        tableScan("tableScan", "table"),
                        expression("a=b")),
                Assignments.of());

        calculator.queryFinished(queryInfo(ImmutableMap.of(node.getId(), 777)), node, session);

        // project cached
        assertThat(calculator.getOutputRowCount(node, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(777d);

        // filter cached based on project row count
        assertThat(calculator.getOutputRowCount(node.getSource(), Lookup.noLookup(), session))
                .isPresent()
                .hasValue(777d);
    }

    @Test
    public void testOnlyDynamicFilter()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        PlanNode dfNode = singleAggregation(
                new PlanNodeId("aggregation"),
                new FilterNode(
                        new PlanNodeId("filter"),
                        tableScan("tableScan", "table"),
                        createDynamicFilterExpression(METADATA, new DynamicFilterId("df"), BIGINT, new SymbolReference("df_symbol"), EQUAL, false)),
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.of(new Symbol("a"))));

        PlanNode noDfNode = singleAggregation(
                new PlanNodeId("aggregation"),
                tableScan("tableScan", "table"),
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.of(new Symbol("a"))));

        // df stats are not there, the dfNode should be cached with exact df
        calculator.queryFinished(queryInfo(ImmutableMap.of(dfNode.getId(), 2)), dfNode, session);

        assertThat(calculator.getOutputRowCount(dfNode, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(2d);

        assertThat(calculator.getOutputRowCount(noDfNode, Lookup.noLookup(), session)).isEmpty();

        calculator.invalidateCache();
        // df is not ALL, the dfNode should be cached with exact df
        calculator.queryFinished(
                queryInfo(
                        ImmutableMap.of(dfNode.getId(), 3),
                        ImmutableMap.of("df", Domain.create(ValueSet.of(BIGINT, 3L), false))),
                dfNode,
                session);

        assertThat(calculator.getOutputRowCount(dfNode, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(3d);

        assertThat(calculator.getOutputRowCount(noDfNode, Lookup.noLookup(), session)).isEmpty();

        calculator.invalidateCache();
        // df is ALL, node with, and without df should be cached
        calculator.queryFinished(
                queryInfo(
                        ImmutableMap.of(dfNode.getId(), 4),
                        ImmutableMap.of("df", Domain.all(BIGINT))),
                dfNode,
                session);

        assertThat(calculator.getOutputRowCount(dfNode, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(4d);
        assertThat(calculator.getOutputRowCount(noDfNode, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(4d);

        calculator.invalidateCache();
        // some other df is ALL, the dfNode should be cached with exact df
        calculator.queryFinished(
                queryInfo(
                        ImmutableMap.of(dfNode.getId(), 5),
                        ImmutableMap.of("other_df", Domain.all(BIGINT))),
                dfNode,
                session);

        assertThat(calculator.getOutputRowCount(dfNode, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(5d);

        assertThat(calculator.getOutputRowCount(noDfNode, Lookup.noLookup(), session)).isEmpty();
    }

    @Test
    public void testDynamicFilterWithOtherFilter()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        PlanNode dfNode = new FilterNode(
                new PlanNodeId("filter"),
                tableScan("tableScan", "table"),
                and(
                        createDynamicFilterExpression(METADATA, new DynamicFilterId("df"), BIGINT, new SymbolReference("df_symbol"), EQUAL, false),
                        expression("a=b")));

        PlanNode noDfNode = new FilterNode(
                new PlanNodeId("filter"),
                tableScan("tableScan", "table"),
                expression("a=b"));
        // df stats are not there, the dfNode should be cached with exact df
        calculator.queryFinished(queryInfo(ImmutableMap.of(dfNode.getId(), 2)), dfNode, session);

        assertThat(calculator.getOutputRowCount(dfNode, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(2d);

        assertThat(calculator.getOutputRowCount(noDfNode, Lookup.noLookup(), session)).isEmpty();

        // df is not ALL, the dfNode should be cached with exact df
        calculator.queryFinished(
                queryInfo(
                        ImmutableMap.of(dfNode.getId(), 3),
                        ImmutableMap.of("df", Domain.create(ValueSet.of(BIGINT, 3L), false))),
                dfNode,
                session);

        assertThat(calculator.getOutputRowCount(dfNode, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(3d);

        assertThat(calculator.getOutputRowCount(noDfNode, Lookup.noLookup(), session)).isEmpty();

        // df is ALL, node with, and without df should be cached
        calculator.queryFinished(
                queryInfo(
                        ImmutableMap.of(dfNode.getId(), 4),
                        ImmutableMap.of("df", Domain.all(BIGINT))),
                dfNode,
                session);

        assertThat(calculator.getOutputRowCount(dfNode, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(4d);
        assertThat(calculator.getOutputRowCount(noDfNode, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(4d);
    }

    @Test
    public void testMultiLevelJoin()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        PlanNode join = join(
                "top_join",
                join(
                        "left_subjoin",
                        new FilterNode(new PlanNodeId("filter1"), tableScan("tableScan", "table"), expression("a=b")),
                        new FilterNode(new PlanNodeId("filter2"), tableScan("tableScan", "table"), expression("c=d"))),
                new FilterNode(new PlanNodeId("filter3"), tableScan("tableScan", "table"), expression("e=f")));

        PlanNode equivalentJoin = join(
                "top_join",
                new FilterNode(new PlanNodeId("filter1"), tableScan("tableScan", "table"), expression("a=b")),
                join(
                        "right_subjoin",
                        new FilterNode(new PlanNodeId("filter2"), tableScan("tableScan", "table"), expression("c=d")),
                        new FilterNode(new PlanNodeId("filter3"), tableScan("tableScan", "table"), expression("e=f"))));

        calculator.queryFinished(queryInfo(ImmutableMap.of(join.getId(), 9)), join, session);

        // original join cached
        assertThat(calculator.getOutputRowCount(join, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(9d);

        // equivalent join cached
        assertThat(calculator.getOutputRowCount(equivalentJoin, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(9d);
    }

    @Test
    public void testPartialAggregation()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        AggregationNode singleAggregation = singleAggregation(new PlanNodeId("aggregation"),
                new FilterNode(new PlanNodeId("filter"), tableScan("tableScan", "table"), expression("a=b")),
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.of(new Symbol("a"))));

        AggregationNode partialAggregation = AggregationNode.builderFrom(singleAggregation)
                .setId(new PlanNodeId("partial"))
                .setStep(PARTIAL)
                .build();
        PlanNode finalAggregation = AggregationNode.builderFrom(singleAggregation)
                .setStep(FINAL)
                .setId(new PlanNodeId("final"))
                .setSource(exchange(
                        "local_exchange",
                        LOCAL,
                        exchange(
                                "remote_exchange",
                                REMOTE,
                                partialAggregation)))
                .build();

        calculator.queryFinished(
                queryInfo(ImmutableMap.of(
                        finalAggregation.getId(), 8,
                        partialAggregation.getId(), 16)),
                finalAggregation,
                session);

        assertThat(calculator.getOutputRowCount(finalAggregation, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(8d);

        assertThat(calculator.getOutputRowCount(partialAggregation, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(16d);

        // single matches the final aggregation
        assertThat(calculator.getOutputRowCount(singleAggregation, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(8d);
    }

    @Test
    public void testPartialAggregationPushedThroughJoin()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        JoinNode join = join(
                "join",
                new FilterNode(new PlanNodeId("filter1"), tableScan("tableScan", "table"), expression("a=b")),
                new FilterNode(new PlanNodeId("filter2"), tableScan("tableScan", "table"), expression("c=d")));
        AggregationNode singleAggregation = singleAggregation(new PlanNodeId("aggregation"),
                join,
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.of(new Symbol("a"))));

        AggregationNode partialAggregation = AggregationNode.builderFrom(singleAggregation)
                .setId(new PlanNodeId("partial"))
                .setStep(PARTIAL)
                .build();
        PlanNode finalAggregation = AggregationNode.builderFrom(singleAggregation)
                .setStep(FINAL)
                .setId(new PlanNodeId("final"))
                .setSource(exchange(
                        "local_exchange",
                        LOCAL,
                        exchange(
                                "remote_exchange",
                                REMOTE,
                                partialAggregation)))
                .build();

        AggregationNode rightPartialAggregation = AggregationNode.builderFrom(singleAggregation)
                .setId(new PlanNodeId("partial"))
                .setStep(PARTIAL)
                .setSource(join.getRight())
                .build();

        JoinNode newJoin = join("new_join", join.getLeft(), rightPartialAggregation);

        AggregationNode intermediateAggregation = AggregationNode.builderFrom(singleAggregation)
                .setId(new PlanNodeId("intermediate"))
                .setStep(INTERMEDIATE)
                .setSource(exchange("local_intermediate_exchange", LOCAL, newJoin))
                .build();

        PlanNode finalAggregationOverNewJoin = AggregationNode.builderFrom(singleAggregation)
                .setStep(FINAL)
                .setId(new PlanNodeId("final"))
                .setSource(exchange(
                        "local_exchange",
                        LOCAL,
                        exchange(
                                "remote_exchange",
                                REMOTE,
                                intermediateAggregation)))
                .build();

        calculator.queryFinished(
                queryInfo(ImmutableMap.of(
                        finalAggregationOverNewJoin.getId(), 1,
                        intermediateAggregation.getId(), 2,
                        newJoin.getId(), 3,
                        rightPartialAggregation.getId(), 4)),
                finalAggregationOverNewJoin,
                session);

        assertThat(calculator.getOutputRowCount(finalAggregationOverNewJoin, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(1d);

        assertThat(calculator.getOutputRowCount(intermediateAggregation, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(2d);

        assertThat(calculator.getOutputRowCount(newJoin, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(3d);

        assertThat(calculator.getOutputRowCount(rightPartialAggregation, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(4d);

        // we don't handle this plan transformation, but we could rewrite intermediate -> join -> partial to partial -> join
        assertThat(calculator.getOutputRowCount(singleAggregation, Lookup.noLookup(), session)).isEmpty();
        assertThat(calculator.getOutputRowCount(finalAggregation, Lookup.noLookup(), session)).isEmpty();
    }

    @Test
    public void testTableScanWithEnforcedConstraint()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        FilterNode node = new FilterNode(new PlanNodeId("filter"), tableScan("tableScan", "table"), expression("a=b"));
        FilterNode enforcedConstraintNode = new FilterNode(
                new PlanNodeId("filter"),
                tableScan("tableScan", "table",
                        TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("a"), Domain.singleValue(BIGINT, 1L)))),
                expression("a=b"));

        calculator.queryFinished(queryInfo(ImmutableMap.of(node.getId(), 777)), enforcedConstraintNode, session);

        assertThat(calculator.getOutputRowCount(enforcedConstraintNode, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(777d);

        // table scan without matching enforcedConstraint does not match
        assertThat(calculator.getOutputRowCount(node, Lookup.noLookup(), session)).isEmpty();
    }

    @Test
    public void testUnion()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        PlanNode node = new UnionNode(
                new PlanNodeId("union"),
                ImmutableList.of(
                        new FilterNode(new PlanNodeId("filter1"), tableScan("tableScan", "table"), expression("a=b")),
                        new FilterNode(new PlanNodeId("filter2"), tableScan("tableScan", "table"), expression("c=d"))),
                ArrayListMultimap.create(),
                ImmutableList.of());

        calculator.queryFinished(queryInfo(ImmutableMap.of(node.getId(), 4)), node, session);

        assertThat(calculator.getOutputRowCount(node, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(4d);
    }

    @Test
    public void testNodesThatDoesNotChangeCardinality()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        FilterNode lastCardinalityChangingNode = new FilterNode(new PlanNodeId("filter"), tableScan("tableScan", "table"), expression("a=b"));
        PlanNode node = new SortNode(
                new PlanNodeId("sort"),
                new ProjectNode(
                        new PlanNodeId("project"),
                        new AssignUniqueId(
                                new PlanNodeId("assignUniqueId"),
                                new MarkDistinctNode(
                                        new PlanNodeId("markDistinct"),
                                        lastCardinalityChangingNode,
                                        new Symbol("marker"),
                                        ImmutableList.of(new Symbol("a")),
                                        Optional.empty()),
                                new Symbol("id")),
                        Assignments.of()),
                new OrderingScheme(ImmutableList.of(new Symbol("a")), ImmutableMap.of(new Symbol("a"), ASC_NULLS_FIRST)),
                false);

        calculator.queryFinished(queryInfo(ImmutableMap.of(lastCardinalityChangingNode.getId(), 777)), node, session);

        assertThat(calculator.getOutputRowCount(node, Lookup.noLookup(), session))
                .isPresent()
                .hasValue(777d);
    }

    @Test
    public void testOnlyFinishedQueriesCached()
    {
        HistoryBasedStatsCalculator calculator = createStatsCalculator();
        Session session = sessionWithHistoryBasedStatsEnabled();

        FilterNode node = new FilterNode(new PlanNodeId("filter"), tableScan("tableScan", "table"), expression("a=b"));

        calculator.queryFinished(queryInfo(FAILED, SELECT, ImmutableMap.of(node.getId(), 777), ImmutableMap.of()), node, session);

        assertThat(calculator.getOutputRowCount(node, Lookup.noLookup(), session)).isEmpty();

        calculator.queryFinished(queryInfo(FINISHED, EXPLAIN, ImmutableMap.of(node.getId(), 777), ImmutableMap.of()), node, session);

        assertThat(calculator.getOutputRowCount(node, Lookup.noLookup(), session)).isEmpty();

        calculator.queryFinished(queryInfo(FINISHED, DESCRIBE, ImmutableMap.of(node.getId(), 777), ImmutableMap.of()), node, session);

        assertThat(calculator.getOutputRowCount(node, Lookup.noLookup(), session)).isEmpty();

        calculator.queryFinished(queryInfo(FINISHED, DATA_DEFINITION, ImmutableMap.of(node.getId(), 777), ImmutableMap.of()), node, session);

        assertThat(calculator.getOutputRowCount(node, Lookup.noLookup(), session)).isEmpty();
    }

    private static Session sessionWithHistoryBasedStatsEnabled()
    {
        return TestingSession.testSessionBuilder()
                .setSystemProperty(HISTORY_BASED_STATISTICS_ENABLED, "true")
                .build();
    }

    private ExchangeNode exchange(String id, ExchangeNode.Scope scope, PlanNode source)
    {
        return ExchangeNode.partitionedExchange(new PlanNodeId(id), scope, source, ImmutableList.of(), Optional.empty());
    }

    private static JoinNode join(String id, PlanNode left, PlanNode right)
    {
        return new JoinNode(
                new PlanNodeId(id),
                JoinNode.Type.INNER,
                left,
                right,
                ImmutableList.of(new JoinNode.EquiJoinClause(left.getOutputSymbols().get(0), right.getOutputSymbols().get(0))),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private static QueryInfo queryInfo(Map<PlanNodeId, Integer> operatorOutputRowCounts)
    {
        return queryInfo(operatorOutputRowCounts, ImmutableMap.of());
    }

    private static QueryInfo queryInfo(Map<PlanNodeId, Integer> operatorOutputRowCounts, Map<String, Domain> dynamicFilterStats)
    {
        return queryInfo(FINISHED, SELECT, operatorOutputRowCounts, dynamicFilterStats);
    }

    private static QueryInfo queryInfo(
            QueryState queryState,
            QueryType queryType,
            Map<PlanNodeId, Integer> operatorOutputRowCounts,
            Map<String, Domain> dynamicFilterStats)
    {
        StageId stageId = new StageId(new QueryId("0"), 0);
        return new QueryInfo(
                new QueryId("0"),
                sessionWithHistoryBasedStatsEnabled().toSessionRepresentation(),
                queryState,
                URI.create("1"),
                ImmutableList.of(),
                "SELECT 1 as number",
                Optional.of("prepared_query"),
                queryStats(dynamicFilterStats),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(TransactionId.create()),
                true,
                "42",
                Optional.of(new StageInfo(stageId,
                        StageState.FINISHED,
                        new PlanFragment(new PlanFragmentId("0"),
                                new ValuesNode(new PlanNodeId("mock"), 0),
                                ImmutableMap.of(),
                                SystemPartitioningHandle.SOURCE_DISTRIBUTION,
                                Optional.empty(),
                                ImmutableList.of(),
                                new PartitioningScheme(Partitioning.create(SOURCE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of()),
                                StatsAndCosts.empty(),
                                ImmutableList.of(),
                                ImmutableList.of(),
                                Optional.empty()),
                        false,
                        ImmutableList.of(),
                        StageStats.createInitial(),
                        ImmutableList.of(taskInfo(stageId, operatorOutputRowCounts)),
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        null)),
                null,
                null,
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                true,
                Optional.empty(),
                Optional.of(queryType),
                RetryPolicy.NONE,
                false,
                new NodeVersion("test"));
    }

    private static QueryStats queryStats(Map<String, Domain> dynamicFilterStats)
    {
        return new QueryStats(
                new DateTime(0),
                new DateTime(0),
                new DateTime(0),
                new DateTime(0),
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                30,
                16,
                17.0,
                18.0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),

                true,
                OptionalDouble.of(8.88),
                OptionalDouble.of(0),
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO,
                false,
                ImmutableSet.of(),

                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                251,
                252,
                Duration.ZERO,
                Duration.ZERO,

                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                253,
                254,

                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                37,
                38,

                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                41,
                42,
                Duration.ZERO,
                Duration.ZERO,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                45,
                46,
                Duration.ZERO,
                Duration.ZERO,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                ImmutableList.of(new StageGcStatistics(
                        101,
                        102,
                        103,
                        104,
                        105,
                        106,
                        107)),
                new DynamicFilterService.DynamicFiltersStats(
                        dynamicFilterStats.entrySet()
                                .stream()
                                .map(entry -> new DynamicFilterDomainStats(new DynamicFilterId(entry.getKey()), entry.getValue().toString(), Optional.empty()))
                                .collect(toImmutableList()),
                        0,
                        0,
                        0,
                        0),
                ImmutableList.of(),
                ImmutableList.of());
    }

    private static TableScanNode tableScan(String id, String table)
    {
        return tableScan(id, table, TupleDomain.none());
    }

    private static TableScanNode tableScan(String id, String table, TupleDomain<ColumnHandle> enforcedConstraint)
    {
        return new TableScanNode(
                new PlanNodeId(id),
                new TableHandle(
                        TEST_CATALOG_HANDLE,
                        new ConnectorTableHandle()
                        {
                            @Override
                            public String toString()
                            {
                                return table;
                            }
                        },
                        TestingTransactionHandle.create()),
                ImmutableList.of(new Symbol(id + "_a"), new Symbol(id + "_b")),
                ImmutableMap.of(new Symbol(id + "_a"), new TestingColumnHandle("a"), new Symbol(id + "_b"), new TestingColumnHandle("b")),
                enforcedConstraint,
                Optional.empty(),
                false,
                Optional.empty());
    }

    private static HistoryBasedStatsCalculator createStatsCalculator()
    {
        return new HistoryBasedStatsCalculator(
                METADATA,
                CACHE_METADATA,
                new ComposableStatsCalculator(ImmutableList.of()),
                new StatsNormalizer());
    }

    private static TaskInfo taskInfo(StageId stageId, Map<PlanNodeId, Integer> operatorOutputRowCounts)
    {
        ImmutableList.Builder<OperatorStats> operatorStats = ImmutableList.builder();
        int operatorId = 0;
        for (Map.Entry<PlanNodeId, Integer> entry : operatorOutputRowCounts.entrySet()) {
            operatorStats.add(operatorStats(entry.getKey(), operatorId++, entry.getValue()));
        }
        operatorStats.add(operatorStats(new PlanNodeId("output"), operatorId++, 0));
        return TaskInfo.createInitialTask(
                new TaskId(stageId, 0, 0),
                URI.create("1"),
                "1",
                false,
                Optional.empty(),
                new TaskStats(DateTime.now(),
                        null,
                        null,
                        null,
                        null,
                        DateTime.now(),
                        Duration.ZERO,
                        Duration.ZERO,
                        0,
                        0,
                        0,
                        0L,
                        0,
                        0,
                        0L,
                        0,
                        0,
                        0.0,
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        Duration.ZERO,
                        Duration.ZERO,
                        Duration.ZERO,
                        false,
                        ImmutableSet.of(),
                        DataSize.ofBytes(0),
                        0,
                        Duration.ZERO,
                        DataSize.ofBytes(0),
                        0,
                        DataSize.ofBytes(0),
                        0,
                        DataSize.ofBytes(0),
                        0,
                        Duration.ZERO,
                        DataSize.ofBytes(0),
                        0,
                        Duration.ZERO,
                        DataSize.ofBytes(0),
                        DataSize.ofBytes(0),
                        Optional.empty(),
                        0,
                        Duration.ZERO,
                        ImmutableList.of(new PipelineStats(
                                0,

                                new DateTime(0),
                                new DateTime(0),
                                new DateTime(0),

                                true,
                                false,

                                1,
                                2,
                                1,
                                21L,
                                3,
                                2,
                                22L,
                                19,
                                4,

                                DataSize.ofBytes(0),
                                DataSize.ofBytes(0),
                                new Distribution().snapshot(),
                                new Distribution().snapshot(),
                                Duration.ZERO,
                                Duration.ZERO,
                                Duration.ZERO,
                                false,
                                ImmutableSet.of(),

                                DataSize.ofBytes(0),
                                151,
                                Duration.ZERO,

                                DataSize.ofBytes(0),
                                152,

                                DataSize.ofBytes(0),
                                15,

                                DataSize.ofBytes(0),
                                17,
                                Duration.ZERO,
                                DataSize.ofBytes(0),
                                19,
                                Duration.ZERO,
                                DataSize.ofBytes(0),
                                operatorStats.build(),
                                ImmutableList.of()))));
    }

    private static OperatorStats operatorStats(PlanNodeId planNodeId, int operatorId, int outputPositions)
    {
        return new OperatorStats(
                0,
                1,
                51,
                operatorId,
                planNodeId,
                "test",
                1,
                2,
                Duration.ZERO,
                Duration.ZERO,
                DataSize.ofBytes(0),
                511,
                Duration.ZERO,
                DataSize.ofBytes(0),
                522,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                7,
                8d,
                9,
                Duration.ZERO,
                Duration.ZERO,
                DataSize.ofBytes(0),
                outputPositions,
                533,
                new Metrics(ImmutableMap.of("metrics", new LongCount(42))),
                new Metrics(ImmutableMap.of("connectorMetrics", new LongCount(43))),

                DataSize.ofBytes(0),

                Duration.ZERO,

                16,
                Duration.ZERO,
                Duration.ZERO,

                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                Optional.empty(),
                null);
    }
}
