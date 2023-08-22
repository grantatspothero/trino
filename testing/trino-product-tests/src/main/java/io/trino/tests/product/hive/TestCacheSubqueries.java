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
package io.trino.tests.product.hive;

import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.CACHE_SUBQUERIES;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheSubqueries
        extends ProductTest
{
    private boolean initialized;

    @BeforeMethodWithContext
    public void init()
    {
        if (initialized) {
            return;
        }
        onTrino().executeQuery("create schema if not exists hive.tpch");
        onTrino().executeQuery("create table if not exists hive.tpch.orders with (partitioned_by = ARRAY['orderpriority']) as select orderkey, orderpriority from tpch.sf1.orders");
        initialized = true;
    }

    @Test(groups = CACHE_SUBQUERIES)
    public void testQueryResulIsValidWhenCacheSubqueriesIsEnabled()
    {
        onTrino().executeQuery("set session cache_subqueries_enabled=false");
        QueryResult expectedResult = onTrino().executeQuery(
                """
                        select count(orderkey) from hive.tpch.orders where orderpriority = '3-MEDIUM'
                        union all
                        select count(orderkey) from hive.tpch.orders where orderpriority = '3-MEDIUM'
                    """);
        onTrino().executeQuery("set session cache_subqueries_enabled=true");

        // cache data
        onTrino().executeQuery(
                """
                        select count(orderkey) from hive.tpch.orders where orderpriority = '3-MEDIUM'
                        union all
                        select count(orderkey) from hive.tpch.orders where orderpriority = '3-MEDIUM'
                    """);
        // use cached data
        QueryResult withCacheEnabledResult = onTrino().executeQuery(
                """
                        select count(orderkey) from hive.tpch.orders where orderpriority = '3-MEDIUM'
                        union all
                        select count(orderkey) from hive.tpch.orders where orderpriority = '3-MEDIUM'
                    """);

        assertThat((long) withCacheEnabledResult.row(0).get(0)).isEqualTo((long) expectedResult.row(0).get(0));
    }
}
