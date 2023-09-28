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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cache.CacheDataOperator;
import io.trino.cache.LoadCachedDataOperator;
import io.trino.operator.OperatorStats;
import io.trino.operator.ScanFilterAndProjectOperator;
import io.trino.operator.TableScanOperator;
import io.trino.spi.QueryId;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static io.trino.SystemSessionProperties.CACHE_SUBQUERIES_ENABLED;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
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
}
