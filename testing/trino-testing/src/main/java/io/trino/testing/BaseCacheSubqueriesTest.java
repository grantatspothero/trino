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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.slice.Slices;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.cache.CacheDataOperator;
import io.trino.cache.CacheMetadata;
import io.trino.cache.LoadCachedDataOperator;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.operator.ScanFilterAndProjectOperator;
import io.trino.operator.TableScanOperator;
import io.trino.operator.dynamicfiltering.DynamicPageFilterCache;
import io.trino.operator.dynamicfiltering.DynamicRowFilteringPageSourceProvider;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.split.SplitSource;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.LoadCachedDataPlanNode;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SystemSessionProperties.CACHE_AGGREGATIONS_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_COMMON_SUBQUERIES_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_PROJECTIONS_ENABLED;
import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.cacheDataPlanNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.chooseAlternativeNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.transaction.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseCacheSubqueriesTest
        extends AbstractTestQueryFramework
{
    protected static final Set<TpchTable<?>> REQUIRED_TABLES = ImmutableSet.of(LINE_ITEM, ORDERS, CUSTOMER);
    protected static final Map<String, String> EXTRA_PROPERTIES = ImmutableMap.of("cache.enabled", "true");

    @BeforeMethod
    public void flushCache()
    {
        getDistributedQueryRunner().getServers().forEach(server -> server.getCacheManagerRegistry().flushCache());
    }

    @DataProvider(name = "isDynamicRowFilteringEnabled")
    public static Object[][] isDynamicRowFilteringEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test
    public void testJoinQuery()
    {
        @Language("SQL") String selectQuery = "select count(l.orderkey) from lineitem l, lineitem r where l.orderkey = r.orderkey";
        MaterializedResultWithQueryId resultWithCache = executeWithQueryId(withCacheEnabled(), selectQuery);
        MaterializedResultWithQueryId resultWithoutCache = executeWithQueryId(withCacheDisabled(), selectQuery);
        assertEqualsIgnoreOrder(resultWithCache.getResult(), resultWithoutCache.getResult());

        // make sure data was read from cache
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.getQueryId())).isPositive();

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.getQueryId())).isPositive();

        // make sure less data is read from source when caching is on
        assertThat(getScanOperatorInputPositions(resultWithCache.getQueryId()))
                .isLessThan(getScanOperatorInputPositions(resultWithoutCache.getQueryId()));
    }

    @Test
    public void testAggregationQuery()
    {
        @Language("SQL") String countQuery = """
                SELECT * FROM
                    (SELECT count(orderkey), orderkey FROM lineitem GROUP BY orderkey) a
                JOIN
                    (SELECT count(orderkey), orderkey FROM lineitem GROUP BY orderkey) b
                ON a.orderkey = b.orderkey""";
        @Language("SQL") String sumQuery = """
                SELECT * FROM
                    (SELECT sum(orderkey), orderkey FROM lineitem GROUP BY orderkey) a
                JOIN
                    (SELECT sum(orderkey), orderkey FROM lineitem GROUP BY orderkey) b
                ON a.orderkey = b.orderkey""";
        MaterializedResultWithQueryId countWithCache = executeWithQueryId(withCacheEnabled(), countQuery);
        MaterializedResultWithQueryId countWithoutCache = executeWithQueryId(withCacheDisabled(), countQuery);
        assertEqualsIgnoreOrder(countWithCache.getResult(), countWithoutCache.getResult());

        // make sure data was read from cache
        assertThat(getLoadCachedDataOperatorInputPositions(countWithCache.getQueryId())).isPositive();

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(countWithCache.getQueryId())).isPositive();

        // make sure less data is read from source when caching is on
        assertThat(getScanOperatorInputPositions(countWithCache.getQueryId()))
                .isLessThan(getScanOperatorInputPositions(countWithoutCache.getQueryId()));

        // subsequent count aggregation query should use cached data only
        countWithCache = executeWithQueryId(withCacheEnabled(), countQuery);
        assertThat(getLoadCachedDataOperatorInputPositions(countWithCache.getQueryId())).isPositive();
        assertThat(getScanOperatorInputPositions(countWithCache.getQueryId())).isZero();

        // subsequent sum aggregation query should read from source as it doesn't match count plan signature
        MaterializedResultWithQueryId sumWithCache = executeWithQueryId(withCacheEnabled(), sumQuery);
        assertThat(getScanOperatorInputPositions(sumWithCache.getQueryId())).isPositive();
    }

    @Test
    public void testSubsequentQueryReadsFromCache()
    {
        @Language("SQL") String selectQuery = "select orderkey from lineitem union all (select orderkey from lineitem union all select orderkey from lineitem)";
        MaterializedResultWithQueryId resultWithCache = executeWithQueryId(withCacheEnabled(), selectQuery);

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.getQueryId())).isPositive();

        resultWithCache = executeWithQueryId(withCacheEnabled(), "select orderkey from lineitem union all select orderkey from lineitem");
        // make sure data was read from cache as data should be cached across queries
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.getQueryId())).isPositive();
        assertThat(getScanOperatorInputPositions(resultWithCache.getQueryId())).isZero();
    }

    @Test(dataProvider = "isDynamicRowFilteringEnabled")
    public void testDynamicFilterCache(boolean isDynamicRowFilteringEnabled)
    {
        createPartitionedTableAsSelect("orders_part", ImmutableList.of("custkey"), "select orderkey, orderdate, orderpriority, mod(custkey, 10) as custkey from orders");
        @Language("SQL") String totalScanOrdersQuery = "select count(orderkey) from orders_part";
        @Language("SQL") String firstJoinQuery = """
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2) t(custkey)) t on o.custkey = t.custkey
                """;
        @Language("SQL") String secondJoinQuery = """
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2, 4) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2, 3) t(custkey)) t on o.custkey = t.custkey
                """;
        @Language("SQL") String thirdJoinQuery = """
                select count(orderkey) from orders_part o join (select * from (values 0, 1) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from orders_part o join (select * from (values 0, 1) t(custkey)) t on o.custkey = t.custkey
                """;

        Session cacheSubqueriesEnabled = withDynamicRowFiltering(withCacheEnabled(), isDynamicRowFilteringEnabled);
        Session cacheSubqueriesDisabled = withDynamicRowFiltering(withCacheDisabled(), isDynamicRowFilteringEnabled);
        MaterializedResultWithQueryId totalScanOrdersExecution = executeWithQueryId(cacheSubqueriesDisabled, totalScanOrdersQuery);
        MaterializedResultWithQueryId firstJoinExecution = executeWithQueryId(cacheSubqueriesEnabled, firstJoinQuery);
        MaterializedResultWithQueryId anotherFirstJoinExecution = executeWithQueryId(cacheSubqueriesEnabled, firstJoinQuery);
        MaterializedResultWithQueryId secondJoinExecution = executeWithQueryId(cacheSubqueriesEnabled, secondJoinQuery);
        MaterializedResultWithQueryId thirdJoinExecution = executeWithQueryId(cacheSubqueriesEnabled, thirdJoinQuery);

        // firstJoinQuery does not read whole probe side as some splits were pruned by dynamic filters
        assertThat(getScanOperatorInputPositions(firstJoinExecution.getQueryId())).isLessThan(getScanOperatorInputPositions(totalScanOrdersExecution.getQueryId()));
        assertThat(getCacheDataOperatorInputPositions(firstJoinExecution.getQueryId())).isPositive();
        // firstJoinQuery reads from table
        assertThat(getScanOperatorInputPositions(firstJoinExecution.getQueryId())).isPositive();
        // second run of firstJoinQuery reads only from cache
        assertThat(getScanOperatorInputPositions(anotherFirstJoinExecution.getQueryId())).isZero();
        assertThat(getLoadCachedDataOperatorInputPositions(anotherFirstJoinExecution.getQueryId())).isPositive();

        // secondJoinQuery reads from table and cache because its predicate is wider that firstJoinQuery's predicate
        assertThat(getCacheDataOperatorInputPositions(secondJoinExecution.getQueryId())).isPositive();
        assertThat(getLoadCachedDataOperatorInputPositions(secondJoinExecution.getQueryId())).isPositive();
        assertThat(getScanOperatorInputPositions(secondJoinExecution.getQueryId())).isPositive();

        // thirdJoinQuery reads only from cache
        assertThat(getLoadCachedDataOperatorInputPositions(thirdJoinExecution.getQueryId())).isPositive();
        assertThat(getScanOperatorInputPositions(thirdJoinExecution.getQueryId())).isZero();

        assertUpdate("drop table orders_part");
    }

    @Test
    public void testPredicateOnPartitioningColumnThatWasNotFullyPushed()
    {
        createPartitionedTableAsSelect("orders_part", ImmutableList.of("orderkey"), "select orderdate, orderpriority, mod(orderkey, 50) as orderkey from orders");
        // mod predicate will be not pushed to connector
        @Language("SQL") String query =
                """
                        select * from (
                            select orderdate from orders_part where orderkey > 5 and mod(orderkey, 10) = 0 and orderpriority = '1-MEDIUM'
                            union all
                            select orderdate from orders_part where orderkey > 10 and mod(orderkey, 10) = 1 and orderpriority = '3-MEDIUM'
                        ) order by orderdate
                        """;
        MaterializedResultWithQueryId cacheDisabledResult = executeWithQueryId(withCacheDisabled(), query);
        executeWithQueryId(withCacheEnabled(), query);
        MaterializedResultWithQueryId cacheEnabledResult = executeWithQueryId(withCacheEnabled(), query);

        assertThat(getLoadCachedDataOperatorInputPositions(cacheEnabledResult.getQueryId())).isPositive();
        assertThat(cacheDisabledResult.getResult()).isEqualTo(cacheEnabledResult.getResult());
        assertUpdate("drop table orders_part");
    }

    @Test
    public void testCacheWhenProjectionsWerePushedDown()
    {
        computeActual("create table orders_with_row (c row(name varchar, lastname varchar, age integer))");
        computeActual("insert into orders_with_row values (row (row ('any_name', 'any_lastname', 25)))");

        @Language("SQL") String query = "select c.name, c.age from orders_with_row union all select c.name, c.age from orders_with_row";
        @Language("SQL") String secondQuery = "select c.lastname, c.age from orders_with_row union all select c.lastname, c.age from orders_with_row";

        Session cacheEnabledProjectionDisabled = withProjectionPushdownEnabled(withCacheEnabled(), false);

        MaterializedResultWithQueryId firstRun = executeWithQueryId(withCacheEnabled(), query);
        assertThat(firstRun.getResult().getRowCount()).isEqualTo(2);
        assertThat(firstRun.getResult().getMaterializedRows().get(0).getFieldCount()).isEqualTo(2);
        assertThat(getCacheDataOperatorInputPositions(firstRun.getQueryId())).isPositive();

        // should use cache
        MaterializedResultWithQueryId secondRun = executeWithQueryId(withCacheEnabled(), query);
        assertThat(secondRun.getResult().getRowCount()).isEqualTo(2);
        assertThat(secondRun.getResult().getMaterializedRows().get(0).getFieldCount()).isEqualTo(2);
        assertThat(getLoadCachedDataOperatorInputPositions(secondRun.getQueryId())).isPositive();

        // shouldn't use cache because selected cacheColumnIds were different in the first case as projections were pushed down
        MaterializedResultWithQueryId pushDownProjectionDisabledRun = executeWithQueryId(cacheEnabledProjectionDisabled, query);
        assertThat(pushDownProjectionDisabledRun.getResult()).isEqualTo(firstRun.getResult());

        // shouldn't use cache because selected columns are different
        MaterializedResultWithQueryId thirdRun = executeWithQueryId(withCacheEnabled(), secondQuery);
        assertThat(getLoadCachedDataOperatorInputPositions(thirdRun.getQueryId())).isLessThanOrEqualTo(1);

        assertUpdate("drop table orders_with_row");
    }

    @Test
    public void testPartitionedQueryCache()
    {
        createPartitionedTableAsSelect("orders_part", ImmutableList.of("orderpriority"), "select orderkey, orderdate, orderpriority from orders");
        @Language("SQL") String selectTwoPartitions = """
                        select orderkey from orders_part where orderpriority IN ('3-MEDIUM', '1-URGENT')
                        union all
                        select orderkey from orders_part where orderpriority IN ('3-MEDIUM', '1-URGENT')
                """;
        @Language("SQL") String selectAllPartitions = """
                        select orderkey from orders_part
                        union all
                        select orderkey from orders_part
                """;
        @Language("SQL") String selectSinglePartition = """
                        select orderkey from orders_part where orderpriority = '3-MEDIUM'
                        union all
                        select orderkey from orders_part where orderpriority = '3-MEDIUM'
                """;

        MaterializedResultWithQueryId twoPartitionsQueryFirst = executeWithQueryId(withCacheEnabled(), selectTwoPartitions);
        Plan twoPartitionsQueryPlan = getDistributedQueryRunner().getQueryPlan(twoPartitionsQueryFirst.getQueryId());
        MaterializedResultWithQueryId twoPartitionsQuerySecond = executeWithQueryId(withCacheEnabled(), selectTwoPartitions);

        MaterializedResultWithQueryId allPartitionsQuery = executeWithQueryId(withCacheEnabled(), selectAllPartitions);
        Plan allPartitionsQueryPlan = getDistributedQueryRunner().getQueryPlan(allPartitionsQuery.getQueryId());

        String catalogId = withTransaction(session -> getDistributedQueryRunner().getCoordinator()
                .getMetadata()
                .getCatalogHandle(session, session.getCatalog().get())
                .orElseThrow()
                .getId());

        PlanSignature signature = new PlanSignature(
                new SignatureKey(catalogId + ":" + getCacheTableId(getSession(), "orders_part")),
                Optional.empty(),
                ImmutableList.of(getCacheColumnId(getSession(), "orders_part", "orderkey")),
                TupleDomain.all(),
                TupleDomain.all());

        PlanMatchPattern chooseAlternativeNode = chooseAlternativeNode(
                tableScan("orders_part"),
                cacheDataPlanNode(tableScan("orders_part")),
                node(LoadCachedDataPlanNode.class)
                        .with(LoadCachedDataPlanNode.class, node -> node.getPlanSignature().equals(signature)));

        PlanMatchPattern originalPlanPattern = anyTree(chooseAlternativeNode, chooseAlternativeNode);

        // predicate for both original plans were pushed down to tableHandle what means that there is no
        // filter nodes. As a result, there is a same plan signatures for both (actually different) queries
        assertPlan(getSession(), twoPartitionsQueryPlan, originalPlanPattern);
        assertPlan(getSession(), allPartitionsQueryPlan, originalPlanPattern);

        // make sure that full scan reads data from table instead of basing on cache even though
        // plan signature is same
        assertThat(getScanOperatorInputPositions(twoPartitionsQueryFirst.getQueryId())).isPositive();
        assertThat(getScanOperatorInputPositions(twoPartitionsQuerySecond.getQueryId())).isZero();
        assertThat(getScanOperatorInputPositions(allPartitionsQuery.getQueryId())).isPositive();

        // notFilteringExecution should read from both cache (for partitions pre-loaded by filtering executions) and
        // from source table
        assertThat(getLoadCachedDataOperatorInputPositions(allPartitionsQuery.getQueryId())).isPositive();

        // single partition query should read from cache only because data for all partitions have been pre-loaded
        MaterializedResultWithQueryId singlePartitionQuery = executeWithQueryId(withCacheEnabled(), selectSinglePartition);
        assertThat(getScanOperatorInputPositions(singlePartitionQuery.getQueryId())).isZero();
        assertThat(getLoadCachedDataOperatorInputPositions(singlePartitionQuery.getQueryId())).isPositive();

        // make sure that adding new partition doesn't invalidate existing cache entries
        computeActual("insert into orders_part values (-42, date '1991-01-01', 'foo')");
        singlePartitionQuery = executeWithQueryId(withCacheEnabled(), selectSinglePartition);
        assertThat(getScanOperatorInputPositions(singlePartitionQuery.getQueryId())).isZero();
        assertThat(getLoadCachedDataOperatorInputPositions(singlePartitionQuery.getQueryId())).isPositive();

        // validate results
        int twoPartitionsRowCount = twoPartitionsQueryFirst.getResult().getRowCount();
        assertThat(twoPartitionsRowCount).isEqualTo(twoPartitionsQuerySecond.getResult().getRowCount());
        assertThat(twoPartitionsRowCount).isLessThan(allPartitionsQuery.getResult().getRowCount());
        assertThat(singlePartitionQuery.getResult().getRowCount()).isLessThan(twoPartitionsRowCount);
        assertUpdate("drop table orders_part");
    }

    @Test(dataProvider = "isDynamicRowFilteringEnabled")
    public void testSimplifyAndPrunePredicate(boolean isDynamicRowFilteringEnabled)
    {
        String tableName = "simplify_and_prune_orders_part_" + isDynamicRowFilteringEnabled;
        createPartitionedTableAsSelect(tableName, ImmutableList.of("orderpriority"), "select orderkey, orderdate, '9876' as orderpriority from orders");
        DistributedQueryRunner runner = getDistributedQueryRunner();
        Session session = withDynamicRowFiltering(
                Session.builder(getSession())
                        .setQueryId(new QueryId("prune_predicate_" + isDynamicRowFilteringEnabled))
                        .build(),
                isDynamicRowFilteringEnabled);
        transaction(runner.getTransactionManager(), runner.getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    TestingTrinoServer server = runner.getCoordinator();
                    String catalog = transactionSession.getCatalog().orElseThrow();
                    Optional<TableHandle> handle = server.getMetadata().getTableHandle(
                            transactionSession,
                            new QualifiedObjectName(catalog, transactionSession.getSchema().orElseThrow(), tableName));
                    assertThat(handle).isPresent();

                    SplitSource splitSource = server.getSplitManager().getSplits(transactionSession, Span.current(), handle.get(), DynamicFilter.EMPTY, alwaysTrue());
                    Split split = getFutureValue(splitSource.getNextBatch(1000)).getSplits().get(0);

                    ColumnHandle partitionColumn = server.getMetadata().getColumnHandles(transactionSession, handle.get()).get("orderpriority");
                    assertThat(partitionColumn).isNotNull();
                    ColumnHandle dataColumn = server.getMetadata().getColumnHandles(transactionSession, handle.get()).get("orderkey");
                    assertThat(dataColumn).isNotNull();

                    ConnectorPageSourceProvider pageSourceProvider = server.getConnector(catalog).getPageSourceProvider();
                    VarcharType type = VarcharType.createVarcharType(4);

                    // simplifyPredicate and prunePredicate should return none if predicate is exclusive on partition column
                    ConnectorSession connectorSession = transactionSession.toConnectorSession(server.getMetadata().getCatalogHandle(transactionSession, catalog).orElseThrow());
                    Domain nonPartitionDomain = Domain.multipleValues(type, Streams.concat(LongStream.range(0, 9_000), LongStream.of(9_999))
                            .boxed()
                            .map(value -> Slices.utf8Slice(value.toString()))
                            .collect(toImmutableList()));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split.getConnectorSplit(),
                            handle.get().getConnectorHandle(),
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, nonPartitionDomain))))
                            .matches(TupleDomain::isNone);
                    assertThat(pageSourceProvider.simplifyPredicate(
                            connectorSession,
                            split.getConnectorSplit(),
                            handle.get().getConnectorHandle(),
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, nonPartitionDomain))))
                            .matches(TupleDomain::isNone);

                    // simplifyPredicate and prunePredicate should prune prefilled column that matches given predicate fully
                    Domain partitionDomain = Domain.singleValue(type, Slices.utf8Slice("9876"));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split.getConnectorSplit(),
                            handle.get().getConnectorHandle(),
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, partitionDomain))))
                            .matches(TupleDomain::isAll);
                    assertThat(pageSourceProvider.simplifyPredicate(
                            connectorSession,
                            split.getConnectorSplit(),
                            handle.get().getConnectorHandle(),
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, partitionDomain))))
                            .matches(TupleDomain::isAll);

                    // prunePredicate should not prune or simplify data column
                    Domain dataDomain = Domain.multipleValues(BIGINT, LongStream.range(0, 10_000)
                            .boxed()
                            .collect(toImmutableList()));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split.getConnectorSplit(),
                            handle.get().getConnectorHandle(),
                            TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                            .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain)));
                    if (isDynamicRowFilteringEnabled) {
                        DynamicRowFilteringPageSourceProvider dynamicRowFilteringPageSourceProvider = new DynamicRowFilteringPageSourceProvider(new DynamicPageFilterCache(new TypeOperators()));
                        // simplifyPredicate should not prune or simplify data column
                        assertThat(dynamicRowFilteringPageSourceProvider.simplifyPredicate(
                                pageSourceProvider,
                                session,
                                connectorSession,
                                split.getConnectorSplit(),
                                handle.get().getConnectorHandle(),
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain)));
                    }
                    else {
                        // simplifyPredicate should not prune but simplify data column
                        assertThat(pageSourceProvider.simplifyPredicate(
                                connectorSession,
                                split.getConnectorSplit(),
                                handle.get().getConnectorHandle(),
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, Domain.create(ValueSet.ofRanges(range(BIGINT, 0L, true, 9_999L, true)), false))));
                    }
                });
        assertUpdate("drop table " + tableName);
    }

    protected CacheColumnId getCacheColumnId(Session session, String tableName, String columnName)
    {
        QueryRunner runner = getQueryRunner();
        QualifiedObjectName table = new QualifiedObjectName(session.getCatalog().orElseThrow(), session.getSchema().orElseThrow(), tableName);
        return transaction(runner.getTransactionManager(), runner.getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    Metadata metadata = runner.getMetadata();
                    CacheMetadata cacheMetadata = runner.getCacheMetadata();
                    TableHandle tableHandle = metadata.getTableHandle(transactionSession, table).get();
                    return new CacheColumnId("[" + cacheMetadata.getCacheColumnId(transactionSession, tableHandle, metadata.getColumnHandles(transactionSession, tableHandle).get(columnName)).get() + "]");
                });
    }

    protected CacheTableId getCacheTableId(Session session, String tableName)
    {
        QueryRunner runner = getQueryRunner();
        QualifiedObjectName table = new QualifiedObjectName(session.getCatalog().orElseThrow(), session.getSchema().orElseThrow(), tableName);
        return transaction(runner.getTransactionManager(), runner.getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    Metadata metadata = runner.getMetadata();
                    CacheMetadata cacheMetadata = runner.getCacheMetadata();
                    TableHandle tableHandle = metadata.getTableHandle(transactionSession, table).get();
                    return cacheMetadata.getCacheTableId(transactionSession, tableHandle).get();
                });
    }

    protected void assertPlan(Session session, Plan plan, PlanMatchPattern pattern)
    {
        QueryRunner runner = getQueryRunner();
        transaction(runner.getTransactionManager(), runner.getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    runner.getTransactionManager().getCatalogHandle(transactionSession.getTransactionId().get(), transactionSession.getCatalog().orElseThrow());
                    PlanAssert.assertPlan(transactionSession, getQueryRunner().getMetadata(), createTestingFunctionManager(), noopStatsCalculator(), plan, pattern);
                });
    }

    protected <T> T withTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return newTransaction().execute(getSession(), transactionSessionConsumer);
    }

    protected MaterializedResultWithQueryId executeWithQueryId(Session session, @Language("SQL") String sql)
    {
        return getDistributedQueryRunner().executeWithQueryId(session, sql);
    }

    protected Long getScanOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, TableScanOperator.class.getSimpleName(), ScanFilterAndProjectOperator.class.getSimpleName());
    }

    protected Long getCacheDataOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, CacheDataOperator.class.getSimpleName());
    }

    protected Long getLoadCachedDataOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, LoadCachedDataOperator.class.getSimpleName());
    }

    protected Long getOperatorInputPositions(QueryId queryId, String... operatorType)
    {
        ImmutableSet<String> operatorTypes = ImmutableSet.copyOf(operatorType);
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> operatorTypes.contains(summary.getOperatorType()))
                .map(OperatorStats::getInputPositions)
                .mapToLong(Long::valueOf)
                .sum();
    }

    protected Session withCacheEnabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(CACHE_COMMON_SUBQUERIES_ENABLED, "true")
                .setSystemProperty(CACHE_AGGREGATIONS_ENABLED, "true")
                .setSystemProperty(CACHE_PROJECTIONS_ENABLED, "true")
                .build();
    }

    protected Session withCacheDisabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(CACHE_COMMON_SUBQUERIES_ENABLED, "false")
                .setSystemProperty(CACHE_AGGREGATIONS_ENABLED, "false")
                .setSystemProperty(CACHE_PROJECTIONS_ENABLED, "false")
                .build();
    }

    protected Session withDynamicRowFiltering(Session baseSession, boolean enabled)
    {
        return Session.builder(baseSession)
                .setSystemProperty("dynamic_row_filtering_enabled", String.valueOf(enabled))
                .build();
    }

    abstract protected void createPartitionedTableAsSelect(String tableName, List<String> partitionColumns, String asSelect);

    protected Session withProjectionPushdownEnabled(Session session, boolean projectionPushdownEnabled)
    {
        return session;
    }
}
