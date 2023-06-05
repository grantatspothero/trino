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
package io.trino.plugin.objectstore;

import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.EXTENDED_STATISTICS_COLLECT_ON_WRITE;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @see TestObjectStoreDeltaFeaturesConnectorTest
 */
public class TestObjectStoreDeltaConnectorTest
        extends BaseObjectStoreConnectorTest
{
    public TestObjectStoreDeltaConnectorTest()
    {
        super(DELTA);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DROP_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_DROP_FIELD:
            case SUPPORTS_SET_COLUMN_TYPE:
            case SUPPORTS_NOT_NULL_CONSTRAINT:
            case SUPPORTS_MATERIALIZED_VIEW_FRESHNESS_FROM_BASE_TABLES:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageMatching("Unable to add '.*' column for: .*")
                .cause()
                .hasMessageMatching(
                        "Transaction log locked.*" +
                                "|.*/_delta_log/\\d+.json already exists" +
                                "|Conflicting concurrent writes found..*" +
                                "|Multiple live locks found for:.*" +
                                "|Target file .* was created during locking");
    }

    @Override
    protected void verifyConcurrentInsertFailurePermissible(Exception e)
    {
        verifyConcurrentUpdateFailurePermissible(e);
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessage("Failed to write Delta Lake transaction log entry")
                .cause()
                .hasMessageMatching(
                        "Transaction log locked.*" +
                                "|.*/_delta_log/\\d+.json already exists" +
                                "|Conflicting concurrent writes found..*" +
                                "|Multiple live locks found for:.*" +
                                "|Target file .* was created during locking");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(1)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time") ||
                typeName.equals("time(6)") ||
                typeName.equals("timestamp") ||
                typeName.equals("timestamp(6)") ||
                typeName.equals("timestamp(6) with time zone") ||
                typeName.equals("char(3)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Delta Lake does not support columns with a default value");
    }

    @Override
    public void testInsertIntoNotNullColumn()
    {
        assertQueryFails(
                "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                "Delta Lake tables do not support NOT NULL columns");
    }

    @Override
    public void testCharVarcharComparison()
    {
        // Delta Lake doesn't have a char type
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasStackTraceContaining("Unsupported type: char(3)");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue()).matches(
                "\\QCREATE TABLE objectstore.tpch.orders (\n" +
                "   orderkey bigint,\n" +
                "   custkey bigint,\n" +
                "   orderstatus varchar,\n" +
                "   totalprice double,\n" +
                "   orderdate date,\n" +
                "   orderpriority varchar,\n" +
                "   clerk varchar,\n" +
                "   shippriority integer,\n" +
                "   comment varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   location = 's3://test-bucket/tpch/orders-\\E.*\\Q',\n" +
                "   type = 'DELTA'\n" +
                ")\\E");
    }

    @Override
    @Test
    public void testUpdateNotNullColumn()
    {
        assertQueryFails(
                "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                format("Delta Lake tables do not support NOT NULL columns"));
    }

    @Override
    public void testHiveSpecificTableProperty()
    {
        assertThatThrownBy(super::testHiveSpecificTableProperty)
                .hasMessage("Table property 'auto_purge' not supported for Delta Lake tables");
    }

    @Override
    public void testIcebergSpecificTableProperty()
    {
        assertThatThrownBy(super::testIcebergSpecificTableProperty)
                .hasMessage("Table property 'partitioning' not supported for Delta Lake tables");
    }

    @Override
    protected Double basicTableStatisticsExpectedNdv(int actualNdv)
    {
        return (double) actualNdv;
    }

    @Test
    public void testOptimize()
    {
        assertUpdate("CREATE TABLE test_optimize (LIKE nation)");
        for (int i = 0; i < 10; i++) {
            assertUpdate("INSERT INTO test_optimize SELECT * FROM nation", "SELECT count(*) FROM nation");
        }
        long fileCount = (long) computeActual("SELECT count(DISTINCT \"$path\") FROM test_optimize").getOnlyValue();

        assertUpdate("ALTER TABLE test_optimize EXECUTE optimize(file_size_threshold => '10kB')");

        long newFileCount = (long) computeActual("SELECT count(DISTINCT \"$path\") FROM test_optimize").getOnlyValue();
        assertThat(newFileCount).isLessThan(fileCount);
    }

    @Override
    public void testAddNotNullColumnToEmptyTable()
    {
        // Override because the connector throws a slightly different error message
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_notnull_col", "(a_varchar varchar)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " ADD COLUMN b_varchar varchar NOT NULL",
                    "Delta Lake tables do not support NOT NULL columns");
        }
    }

    @Test
    public void testCreateTableAsStatistics()
    {
        String tableName = "test_ctats_stats_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");
    }

    @Test
    public void testAnalyze()
    {
        Session sessionWithDisabledStatisticsOnWrite = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), EXTENDED_STATISTICS_COLLECT_ON_WRITE, "false")
                .build();

        String tableName = "test_analyze_" + randomNameSuffix();
        assertUpdate(sessionWithDisabledStatisticsOnWrite, "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, null, 0.0, null, 0, 24)," +
                        "('regionkey', null, null, 0.0, null, 0, 4)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        runAnalyzeVerifySplitCount(tableName, 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // reanalyze data (1 split is empty values)
        runAnalyzeVerifySplitCount(tableName, 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // insert one more copy; should not influence stats other than rowcount
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        runAnalyzeVerifySplitCount(tableName, 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 3714.0, 25.0, 0.0, null, null, null)," +
                        "('name', 354.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', 5571.0, 50.0, 0.0, null, null, null)," +
                        "('name', 531.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");

        // with analyze we should get new size and NDV
        runAnalyzeVerifySplitCount(tableName, 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, 0, 9)," +
                        "('comment', 5571.0, 50.0, 0.0, null, null, null)," +
                        "('name', 531.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");
    }

    @Test
    public void testAnalyzePartitioned()
    {
        Session sessionWithDisabledStatisticsOnWrite = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), EXTENDED_STATISTICS_COLLECT_ON_WRITE, "false")
                .build();

        String tableName = "test_analyze_" + randomNameSuffix();
        assertUpdate(sessionWithDisabledStatisticsOnWrite, "CREATE TABLE " + tableName
                + " WITH ("
                + "   partitioned_by = ARRAY['regionkey']"
                + ")"
                + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, null, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        runAnalyzeVerifySplitCount(tableName, 5);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        // insert one more copy; should not influence stats other than rowcount
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        assertUpdate("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, null, null)," +
                        "('comment', 3714.0, 25.0, 0.0, null, null, null)," +
                        "('name', 354.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 50.0, null, null)");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, null, null)," +
                        "('comment', 5571.0, 50.0, 0.0, null, null, null)," +
                        "('name', 531.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");

        // with analyze we should get new size and NDV
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50.0, 0.0, null, 0, 49)," +
                        "('regionkey', null, 10.0, 0.0, null, null, null)," +
                        "('comment', 5571.0, 50.0, 0.0, null, null, null)," +
                        "('name', 531.0, 50.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 75.0, null, null)");
    }

    private void runAnalyzeVerifySplitCount(String tableName, long expectedSplitCount)
    {
        MaterializedResultWithQueryId analyzeResult = getDistributedQueryRunner().executeWithQueryId(getSession(), "ANALYZE " + tableName);
        verifySplitCount(analyzeResult.getQueryId(), expectedSplitCount);
    }

    private void verifySplitCount(QueryId queryId, long expectedCount)
    {
        OperatorStats operatorStats = getOperatorStats(queryId);
        assertThat(operatorStats.getTotalDrivers()).isEqualTo(expectedCount);
    }

    private OperatorStats getOperatorStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().contains("Scan"))
                .collect(onlyElement());
    }
}
