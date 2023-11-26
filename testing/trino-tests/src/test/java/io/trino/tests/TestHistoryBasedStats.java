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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.cost.HistoryBasedStatsCalculator;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.airlift.log.Level.DEBUG;
import static io.airlift.log.Level.INFO;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SystemSessionProperties.HISTORY_BASED_STATISTICS_ENABLED;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestHistoryBasedStats
        extends AbstractTestQueryFramework
{
    private final Session memorySession = testSessionBuilder()
            .setCatalog("memory")
            .setSchema("default")
            .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
            .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name())
            .setSystemProperty(HISTORY_BASED_STATISTICS_ENABLED, "true")
            .build();

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

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build();

        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory", ImmutableMap.of("memory.enable-cache-ids", "true"));

        // create test tables with column that has non-uniformly distributed (skewed) values
        queryRunner.execute(memorySession, """
                CREATE TABLE three_values AS (
                    (SELECT random(1000000000000000) AS id, 1 AS value FROM tpch.sf1.lineitem limit 6000000)
                    UNION ALL
                    (SELECT random(1000000000000000) AS id, 2 AS value FROM tpch.sf1.lineitem limit 200000)
                    UNION ALL
                    (SELECT random(1000000000000000) AS id, 3 AS value FROM tpch.sf1.lineitem limit 100))
                 """);

        queryRunner.execute(memorySession, "CREATE TABLE two_values AS SELECT id, value as value2 FROM three_values WHERE value IN (1, 2)");

        return queryRunner;
    }

    @BeforeEach
    public void flushCache()
    {
        assertQuerySucceeds("call system.system.flush_history_based_stats_cache()");
    }

    @Test
    public void testBetterJoinOrderWithSkewedFilter()
    {
        // first run, the join has bad join order
        assertQuery(
                memorySession,
                """
                        SELECT count(*)
                        FROM two_values two, three_values three
                        WHERE two.id = three.id AND two.value2 = 2 AND three.value = 1
                        """,
                "VALUES (0)",
                plan -> assertThat(buildSideTable(plan)).isEqualTo("three_values"));

        // the second run should use cached stats and have the correct join order
        assertQuery(
                memorySession,
                """
                        SELECT count(*)
                        FROM two_values two, three_values three
                        WHERE two.id = three.id AND two.value2 = 2 AND three.value = 1
                        """,
                "VALUES (0)",
                plan -> assertThat(buildSideTable(plan)).isEqualTo("two_values"));
    }

    @Test
    public void testBetterJoinOrderWithSkewedFilterUsingCrossQueryStats()
    {
        // make sure the expected build side row count is cached
        assertQuery(memorySession, "select count(*) from two_values where value2 = 2", "VALUES (200000)");
        // make sure the expected probe side row count is cached
        assertQuery(memorySession, "select count(*) from three_values where value = 1", "VALUES (6000000)");

        // the second run should use cached stats and have the correct join order
        assertQuery(
                memorySession,
                """
                        SELECT count(*)
                        FROM two_values two, three_values three
                        WHERE two.id = three.id AND two.value2 = 2 AND three.value = 1
                        """,
                "VALUES (0)",
                plan -> assertThat(buildSideTable(plan)).isEqualTo("two_values"));
    }

    @Test
    public void testBetterJoinDistributionTypeWithSkewedFilter()
    {
        Session session = Session.builder(memorySession)
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, DataSize.of(1, MEGABYTE).toString())
                .build();
        // make sure the probe side row count is cached
        assertQuery(session, "SELECT count(*) FROM two_values WHERE value2 = 1", "VALUES (6000000)");
        // first run, the join has a bad join distribution type
        assertQuery(
                session,
                """
                        SELECT count(*)
                        FROM two_values two, three_values three
                        WHERE two.id = three.id AND two.value2 = 1 AND three.value = 3
                        """,
                "VALUES (0)",
                plan -> assertThat(joinDistributionType(plan)).isEqualTo(PARTITIONED));

        // the second run should use cached stats, and now the replicated join distribution type is chosen
        assertQuery(
                session,
                """
                        SELECT count(*)
                        FROM two_values two, three_values three
                        WHERE two.id = three.id AND two.value2 = 1 AND three.value = 3
                        """,
                "VALUES (0)",
                plan -> assertThat(joinDistributionType(plan)).isEqualTo(REPLICATED));
    }

    private static DistributionType joinDistributionType(Plan plan)
    {
        JoinNode join = searchFrom(plan.getRoot()).whereIsInstanceOfAny(JoinNode.class).findOnlyElement();
        return join.getDistributionType().orElseThrow();
    }

    private String buildSideTable(Plan plan)
    {
        JoinNode join = searchFrom(plan.getRoot()).whereIsInstanceOfAny(JoinNode.class).findOnlyElement();
        TableScanNode buildSideTableScan = searchFrom(join.getRight()).whereIsInstanceOfAny(TableScanNode.class).findOnlyElement();
        QueryRunner queryRunner = getQueryRunner();
        return transaction(queryRunner.getTransactionManager(), queryRunner.getMetadata(), queryRunner.getAccessControl())
                .execute(memorySession, transactionalSession -> {
                    queryRunner.getMetadata().getCatalogHandle(transactionalSession, transactionalSession.getCatalog().get());
                    return queryRunner.getMetadata().getTableName(transactionalSession, buildSideTableScan.getTable()).getSchemaTableName().getTableName();
                });
    }
}
