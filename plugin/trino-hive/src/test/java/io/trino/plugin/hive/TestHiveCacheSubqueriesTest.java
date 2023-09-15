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
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.operator.TableScanOperator;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.LoadCachedDataPlanNode;
import io.trino.testing.BaseCacheSubqueriesTest;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.cacheDataPlanNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.chooseAlternativeNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.transaction.TransactionBuilder.transaction;
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

    @Test
    public void testPartitionedQueryCache()
    {
        computeActual("create table orders_part with (partitioned_by = ARRAY['orderpriority']) as select orderkey, orderdate, orderpriority from orders");
        @Language("SQL") String filteringQuery = """
                        select orderkey from orders_part where orderpriority = '3-MEDIUM'
                        union all
                        select orderkey from orders_part where orderpriority = '3-MEDIUM'
                """;
        @Language("SQL") String notFilteringQuery = """
                        select orderkey from orders_part
                        union all
                        select orderkey from orders_part
                """;
        MaterializedResultWithQueryId filteringExecutionFirst = executeWithQueryId(withCacheSubqueriesEnabled(), filteringQuery);
        Plan filteringPlanFirst = getDistributedQueryRunner().getQueryPlan(filteringExecutionFirst.getQueryId());
        MaterializedResultWithQueryId filteringExecutionSecond = executeWithQueryId(withCacheSubqueriesEnabled(), filteringQuery);

        MaterializedResultWithQueryId notFilteringExecution = executeWithQueryId(withCacheSubqueriesEnabled(), notFilteringQuery);
        Plan notFilteringPlan = getDistributedQueryRunner().getQueryPlan(notFilteringExecution.getQueryId());

        PlanSignature signature = new PlanSignature(
                new SignatureKey("hive:normal:ecbc7bd9e9123daf864b27beb2f8fb6340059b079e3cbce035fdf754c0bca1de:{\"schemaName\":\"tpch\",\"tableName\":\"orders_part\",\"compactEffectivePredicate\":{\"columnDomains\":[]}}"),
                Optional.empty(),
                ImmutableList.of(getCacheColumnId(getSession(), new QualifiedObjectName("hive", "tpch", "orders_part"), "orderkey")),
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
        assertPlan(getSession(), filteringPlanFirst, originalPlanPattern);
        assertPlan(getSession(), notFilteringPlan, originalPlanPattern);

        // make sure that full scan reads data from table instead of basing on cache even though
        // plan signature is same
        assertThat(getScanOperatorInputPositions(filteringExecutionFirst.getQueryId())).isPositive();
        assertThat(getScanOperatorInputPositions(filteringExecutionSecond.getQueryId())).isZero();
        assertThat(getScanOperatorInputPositions(notFilteringExecution.getQueryId())).isPositive();

        // notFilteringExecution should read from both cache (for partitions pre-loaded by filtering executions) and
        // from source table
        assertThat(getLoadCachedDataOperatorInputPositions(notFilteringExecution.getQueryId())).isPositive();

        // validate results
        int count = filteringExecutionFirst.getResult().getRowCount();
        assertThat(count).isEqualTo(filteringExecutionSecond.getResult().getRowCount());
        assertThat(count).isLessThan(notFilteringExecution.getResult().getRowCount());
        assertUpdate("drop table orders_part");
    }

    private CacheColumnId getCacheColumnId(Session session, QualifiedObjectName table, String columnName)
    {
        QueryRunner runner = getQueryRunner();
        return transaction(runner.getTransactionManager(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    Metadata metadata = runner.getMetadata();
                    TableHandle tableHandle = metadata.getTableHandle(transactionSession, table).get();
                    return new CacheColumnId("[" + metadata.getCacheColumnId(transactionSession, tableHandle, metadata.getColumnHandles(transactionSession, tableHandle).get(columnName)).get() + "]");
                });
    }

    private void assertPlan(Session session, Plan plan, PlanMatchPattern pattern)
    {
        QueryRunner runner = getQueryRunner();
        transaction(runner.getTransactionManager(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    PlanAssert.assertPlan(transactionSession, getQueryRunner().getMetadata(), createTestingFunctionManager(), noopStatsCalculator(), plan, pattern);
                });
    }
}
