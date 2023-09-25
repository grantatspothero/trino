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
import io.trino.Session;
import io.trino.cache.CacheDataOperator;
import io.trino.cache.LoadCachedDataOperator;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.operator.ScanFilterAndProjectOperator;
import io.trino.operator.TableScanOperator;
import io.trino.spi.QueryId;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.LoadCachedDataPlanNode;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.trino.SystemSessionProperties.CACHE_SUBQUERIES_ENABLED;
import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.cacheDataPlanNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.chooseAlternativeNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.transaction.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseCacheSubqueriesTest
        extends AbstractTestQueryFramework
{
    protected static final Set<TpchTable<?>> REQUIRED_TABLES = ImmutableSet.of(LINE_ITEM, ORDERS);
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
        MaterializedResultWithQueryId resultWithCache = executeWithQueryId(withCacheSubqueriesEnabled(), selectQuery);
        MaterializedResultWithQueryId resultWithoutCache = executeWithQueryId(withCacheSubqueriesDisabled(), selectQuery);
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
        MaterializedResultWithQueryId countWithCache = executeWithQueryId(withCacheSubqueriesEnabled(), countQuery);
        MaterializedResultWithQueryId countWithoutCache = executeWithQueryId(withCacheSubqueriesDisabled(), countQuery);
        assertEqualsIgnoreOrder(countWithCache.getResult(), countWithoutCache.getResult());

        // make sure data was read from cache
        assertThat(getLoadCachedDataOperatorInputPositions(countWithCache.getQueryId())).isPositive();

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(countWithCache.getQueryId())).isPositive();

        // make sure less data is read from source when caching is on
        assertThat(getScanOperatorInputPositions(countWithCache.getQueryId()))
                .isLessThan(getScanOperatorInputPositions(countWithoutCache.getQueryId()));

        // subsequent count aggregation query should use cached data only
        countWithCache = executeWithQueryId(withCacheSubqueriesEnabled(), countQuery);
        assertThat(getLoadCachedDataOperatorInputPositions(countWithCache.getQueryId())).isPositive();
        assertThat(getScanOperatorInputPositions(countWithCache.getQueryId())).isZero();

        // subsequent sum aggregation query should read from source as it doesn't match count plan signature
        MaterializedResultWithQueryId sumWithCache = executeWithQueryId(withCacheSubqueriesEnabled(), sumQuery);
        assertThat(getScanOperatorInputPositions(sumWithCache.getQueryId())).isPositive();
    }

    @Test
    public void testSubsequentQueryReadsFromCache()
    {
        @Language("SQL") String selectQuery = "select orderkey from lineitem union all (select orderkey from lineitem union all select orderkey from lineitem)";
        MaterializedResultWithQueryId resultWithCache = executeWithQueryId(withCacheSubqueriesEnabled(), selectQuery);

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.getQueryId())).isPositive();

        resultWithCache = executeWithQueryId(withCacheSubqueriesEnabled(), "select orderkey from lineitem union all select orderkey from lineitem");
        // make sure data was read from cache as data should be cached across queries
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.getQueryId())).isPositive();
        assertThat(getScanOperatorInputPositions(resultWithCache.getQueryId())).isZero();
    }

    @Test(dataProvider = "isDynamicRowFilteringEnabled")
    public void testDynamicFilterCache(boolean isDynamicRowFilteringEnabled)
    {
        computeActual("create table orders_part with (partitioned_by = ARRAY['custkey']) as select orderkey, orderdate, orderpriority, mod(custkey, 10) as custkey from orders");

        @Language("SQL") String totalScanOrdersQuery = "select count(orderkey) from orders_part";
        @Language("SQL") String firstJoinQuery =
                """
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2) t(custkey)) t on o.custkey = t.custkey
                """;
        @Language("SQL") String secondJoinQuery =
                """
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2, 4) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from orders_part o join (select * from (values 0, 1, 2, 3) t(custkey)) t on o.custkey = t.custkey
                """;
        @Language("SQL") String thirdJoinQuery =
                """
                select count(orderkey) from orders_part o join (select * from (values 0, 1) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from orders_part o join (select * from (values 0, 1) t(custkey)) t on o.custkey = t.custkey
                """;

        Session cacheSubqueriesEnabled = withDynamicRowFiltering(withCacheSubqueriesEnabled(), isDynamicRowFilteringEnabled);
        Session cacheSubqueriesDisabled = withDynamicRowFiltering(withCacheSubqueriesDisabled(), isDynamicRowFilteringEnabled);
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
        computeActual("create table orders_part with (partitioned_by = ARRAY['orderkey']) as select orderdate, orderpriority, mod(orderkey, 50) as orderkey from orders");
        // mod predicate will be not pushed to connector
        @Language("SQL") String query =
                """
                        select * from (
                            select orderdate from orders_part where orderkey > 5 and mod(orderkey, 10) = 0 and orderpriority = '1-MEDIUM'
                            union all
                            select orderdate from orders_part where orderkey > 10 and mod(orderkey, 10) = 1 and orderpriority = '3-MEDIUM'
                        ) order by orderdate
                        """;
        MaterializedResultWithQueryId cacheDisabledResult = executeWithQueryId(withCacheSubqueriesDisabled(), query);
        executeWithQueryId(withCacheSubqueriesEnabled(), query);
        MaterializedResultWithQueryId cacheEnabledResult = executeWithQueryId(withCacheSubqueriesEnabled(), query);

        assertThat(getLoadCachedDataOperatorInputPositions(cacheEnabledResult.getQueryId())).isPositive();
        assertThat(cacheDisabledResult.getResult()).isEqualTo(cacheEnabledResult.getResult());
        assertUpdate("drop table orders_part");
    }

    @Test
    public void testPartitionedQueryCache()
    {
        computeActual("create table orders_part with (partitioned_by = ARRAY['orderpriority']) as select orderkey, orderdate, orderpriority from orders");
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

        MaterializedResultWithQueryId twoPartitionsQueryFirst = executeWithQueryId(withCacheSubqueriesEnabled(), selectTwoPartitions);
        Plan twoPartitionsQueryPlan = getDistributedQueryRunner().getQueryPlan(twoPartitionsQueryFirst.getQueryId());
        MaterializedResultWithQueryId twoPartitionsQuerySecond = executeWithQueryId(withCacheSubqueriesEnabled(), selectTwoPartitions);

        MaterializedResultWithQueryId allPartitionsQuery = executeWithQueryId(withCacheSubqueriesEnabled(), selectAllPartitions);
        Plan allPartitionsQueryPlan = getDistributedQueryRunner().getQueryPlan(allPartitionsQuery.getQueryId());

        String hiveCatalogId = withTransaction(session -> getDistributedQueryRunner().getCoordinator()
                .getMetadata()
                .getCatalogHandle(session, session.getCatalog().get())
                .orElseThrow()
                .getId());

        PlanSignature signature = new PlanSignature(
                new SignatureKey(hiveCatalogId + ":{\"schemaName\":\"tpch\",\"tableName\":\"orders_part\",\"compactEffectivePredicate\":{\"columnDomains\":[]}}"),
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
        MaterializedResultWithQueryId singlePartitionQuery = executeWithQueryId(withCacheSubqueriesEnabled(), selectSinglePartition);
        assertThat(getScanOperatorInputPositions(singlePartitionQuery.getQueryId())).isZero();
        assertThat(getLoadCachedDataOperatorInputPositions(singlePartitionQuery.getQueryId())).isPositive();

        // validate results
        int twoPartitionsRowCount = twoPartitionsQueryFirst.getResult().getRowCount();
        assertThat(twoPartitionsRowCount).isEqualTo(twoPartitionsQuerySecond.getResult().getRowCount());
        assertThat(twoPartitionsRowCount).isLessThan(allPartitionsQuery.getResult().getRowCount());
        assertThat(singlePartitionQuery.getResult().getRowCount()).isLessThan(twoPartitionsRowCount);
        assertUpdate("drop table orders_part");
    }

    protected CacheColumnId getCacheColumnId(Session session, String tableName, String columnName)
    {
        QueryRunner runner = getQueryRunner();
        QualifiedObjectName table = new QualifiedObjectName(session.getCatalog().orElseThrow(), session.getSchema().orElseThrow(), tableName);
        return transaction(runner.getTransactionManager(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    Metadata metadata = runner.getMetadata();
                    TableHandle tableHandle = metadata.getTableHandle(transactionSession, table).get();
                    return new CacheColumnId("[" + metadata.getCacheColumnId(transactionSession, tableHandle, metadata.getColumnHandles(transactionSession, tableHandle).get(columnName)).get() + "]");
                });
    }

    protected void assertPlan(Session session, Plan plan, PlanMatchPattern pattern)
    {
        QueryRunner runner = getQueryRunner();
        transaction(runner.getTransactionManager(), runner.getAccessControl())
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

    protected Session withCacheSubqueriesEnabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(CACHE_SUBQUERIES_ENABLED, "true")
                .build();
    }

    protected Session withCacheSubqueriesDisabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(CACHE_SUBQUERIES_ENABLED, "false")
                .build();
    }

    protected Session withDynamicRowFiltering(Session baseSession, boolean enabled)
    {
        return Session.builder(baseSession)
                .setCatalogSessionProperty(baseSession.getCatalog().get(), "dynamic_row_filtering_enabled", String.valueOf(enabled))
                .build();
    }
}
