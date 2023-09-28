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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.operator.TableScanOperator;
import io.trino.testing.BaseCacheSubqueriesTest;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestHiveCacheSubqueriesTest
        extends BaseCacheSubqueriesTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setExtraProperties(EXTRA_PROPERTIES)
                .setInitialTables(REQUIRED_TABLES)
                .build();
    }

    @Test
    public void testDoNotUseCacheForNewData()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_do_not_use_cache",
                "(name VARCHAR)",
                ImmutableList.of(
                        "'value1'",
                        "'value2'"))) {
            @Language("SQL") String selectQuery = "select name from %s union all select name from %s".formatted(testTable.getName(), testTable.getName());

            MaterializedResultWithQueryId result = executeWithQueryId(withCacheSubqueriesEnabled(), selectQuery);
            assertThat(result.getResult().getRowCount()).isEqualTo(4);
            assertThat(getOperatorInputPositions(result.getQueryId(), TableScanOperator.class.getSimpleName())).isPositive();

            assertUpdate("insert into %s(name) values ('value3')".formatted(testTable.getName()), 1);
            result = executeWithQueryId(withCacheSubqueriesEnabled(), selectQuery);

            // make sure that if underlying data was changed the second query sees changes
            // and data was read from both table (newly inserted data) and from cache (existing data)
            assertThat(result.getResult().getRowCount()).isEqualTo(6);
            assertThat(getLoadCachedDataOperatorInputPositions(result.getQueryId())).isPositive();
            assertThat(getScanOperatorInputPositions(result.getQueryId())).isPositive();
        }
    }
}
