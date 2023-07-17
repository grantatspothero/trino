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
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseCacheSubqueriesTest
        extends AbstractTestQueryFramework
{
    protected static final Set<TpchTable<?>> REQUIRED_TABLES = ImmutableSet.of(LINE_ITEM);
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

    private MaterializedResultWithQueryId executeWithQueryId(Session session, @Language("SQL") String sql)
    {
        return getDistributedQueryRunner().executeWithQueryId(session, sql);
    }

    private Long getScanOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, TableScanOperator.class.getSimpleName(), ScanFilterAndProjectOperator.class.getSimpleName());
    }

    private Long getCacheDataOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, CacheDataOperator.class.getSimpleName());
    }

    private Long getLoadCachedDataOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, LoadCachedDataOperator.class.getSimpleName());
    }

    private Long getOperatorInputPositions(QueryId queryId, String... operatorType)
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

    private Session withCacheSubqueriesEnabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(CACHE_SUBQUERIES_ENABLED, "true")
                .build();
    }

    private Session withCacheSubqueriesDisabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(CACHE_SUBQUERIES_ENABLED, "false")
                .build();
    }
}
