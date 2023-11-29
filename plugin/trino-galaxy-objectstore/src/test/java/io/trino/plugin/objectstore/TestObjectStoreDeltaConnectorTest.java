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
import io.trino.plugin.deltalake.TestDeltaLakeConnectorTest;
import io.trino.spi.QueryId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.EXTENDED_STATISTICS_COLLECT_ON_WRITE;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
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
    @SuppressWarnings("DuplicateBranchesInSwitch")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        boolean connectorHasBehavior = new GetDeltaLakeConnectorTestBehavior().hasBehavior(connectorBehavior);

        switch (connectorBehavior) {
            case SUPPORTS_DROP_SCHEMA_CASCADE:
                return true;

            case SUPPORTS_RENAME_SCHEMA: // ObjectStore supports this via Hive connector
                // when this fails remove the `case` for given flag
                verify(!connectorHasBehavior, "Unexpected support for: %s", connectorBehavior);
                return true;

            // ObjectStore adds support for materialized views using Iceberg
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW_GRACE_PERIOD:
            case SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW:
//            case SUPPORTS_MATERIALIZED_VIEW_FRESHNESS_FROM_BASE_TABLES: TODO currently not supported for Iceberg materialized views based on Delta tables
            case SUPPORTS_RENAME_MATERIALIZED_VIEW:
//            case SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS: -- not supported by Iceberg
            case SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN:
                // when this fails remove the `case` for given flag
                verify(!connectorHasBehavior, "Unexpected support for: %s", connectorBehavior);
                return true;

            case SUPPORTS_CREATE_FUNCTION:
                return false;

            default:
                // By default, declare all behaviors/features supported by Delta connector
                return connectorHasBehavior;
        }
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessage("Failed to write Delta Lake transaction log entry")
                .cause()
                .hasMessageMatching(transactionConflictErrors());
    }

    @Override
    protected void verifyConcurrentInsertFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessage("Failed to write Delta Lake transaction log entry")
                .cause()
                .hasMessageMatching(transactionConflictErrors());
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageMatching("Unable to add '.*' column for: .*")
                .cause()
                .hasMessageMatching(transactionConflictErrors());
    }

    @Language("RegExp")
    private static String transactionConflictErrors()
    {
        return TestDeltaLakeConnectorTest.transactionConflictErrors();
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "NULL value not allowed for NOT NULL column: " + columnName;
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Unable to add NOT NULL column '.*' for non-empty table: .*");
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
    public void testHiveSpecificTableProperty()
    {
        assertThatThrownBy(super::testHiveSpecificTableProperty)
                .hasMessage("Table property 'auto_purge' not supported for Delta Lake tables");
    }

    @Override
    public void testHiveSpecificColumnProperty()
    {
        assertThatThrownBy(() -> query("""
                CREATE TABLE test_hive_specific_column_property(
                   xyz bigint,
                   abc bigint WITH (partition_projection_type = 'INTEGER', partition_projection_range = ARRAY['0', '10'])
                )"""))
                .hasMessage("Delta Lake tables do not support column properties [partition_projection_type, partition_projection_range]");
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

    @Override
    public void testDropColumn()
    {
        // Override because the connector doesn't support dropping columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(super::testDropColumn)
                .hasMessageContaining("Cannot drop column from table using column mapping mode NONE");
    }

    @Override
    public void testAddAndDropColumnName(String columnName)
    {
        // Override because the connector doesn't support dropping columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(() -> super.testAddAndDropColumnName(columnName))
                .hasMessageContaining("Cannot drop column from table using column mapping mode NONE");
    }

    @Override
    public void testDropAndAddColumnWithSameName()
    {
        // Override because the connector doesn't support dropping columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(super::testDropAndAddColumnWithSameName)
                .hasMessageContaining("Cannot drop column from table using column mapping mode NONE");
    }

    @Override
    public void testRenameColumn()
    {
        // Override because the connector doesn't support renaming columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(super::testRenameColumn)
                .hasMessageContaining("Cannot rename column in table using column mapping mode NONE");
    }

    @Override
    public void testRenameColumnWithComment()
    {
        // Override because the connector doesn't support renaming columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(super::testRenameColumnWithComment)
                .hasMessageContaining("Cannot rename column in table using column mapping mode NONE");
    }

    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        // Override because the connector doesn't support renaming columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(super::testAlterTableRenameColumnToLongName)
                .hasMessageContaining("Cannot rename column in table using column mapping mode NONE");
    }

    @Override
    public void testRenameColumnName(String columnName)
    {
        // Override because the connector doesn't support renaming columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(() -> super.testRenameColumnName(columnName))
                .hasMessageContaining("Cannot rename column in table using column mapping mode NONE");
    }

    @Override
    public void testRenameRowField()
    {
        // Renaming row field is not supported, but a non-standard exception message is thrown.
        assertThatThrownBy(super::testRenameRowField)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("""

                        Expecting message:
                          "Renaming fields in Delta Lake tables is not supported"
                        to match regex:
                          "This connector does not support renaming fields"
                        but did not.""");
    }

    @Override
    public void testSetFieldType()
    {
        // Setting row field type is not supported, but a non standard exception message is thrown.
        assertThatThrownBy(super::testSetFieldType)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("""

                        Expecting message:
                          "Setting field type in Delta Lake tables is not supported"
                        to match regex:
                          "This connector does not support setting field types"
                        but did not.""");
    }

    @Override
    protected Session withoutSmallFileThreshold(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_small_file_threshold", "0B")
                .build();
    }

    /**
     * @see deltalake.partition_values_parsed
     */
    @Test
    public void testDeltaLakeWithPartitionValuesParsed()
    {
        testPartitionValuesParsed("deltalake/partition_values_parsed");
    }

    /**
     * @see trino432.partition_values_parsed
     */
    @Test
    public void testTrinoWithoutPartitionValuesParsed()
    {
        testPartitionValuesParsed("trino432/partition_values_parsed");
    }

    private void testPartitionValuesParsed(String resourceName)
    {
        String tableName = "test_partition_values_parsed_checkpoint_" + randomNameSuffix();
        String tableLocation = "s3://test-bucket/" + tableName;

        minio.copyResources(resourceName, "test-bucket", tableName);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation));

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "checkpoint_filtering_enabled", "true")
                .build();

        assertThat(query(session, "SELECT id FROM " + tableName + " WHERE int_part = 10 AND string_part = 'part1'"))
                .matches("VALUES 1");
        assertThat(query(session, "SELECT id FROM " + tableName + " WHERE int_part != 10"))
                .matches("VALUES 2");
        assertThat(query(session, "SELECT id FROM " + tableName + " WHERE int_part > 10"))
                .matches("VALUES 2");
        assertThat(query(session, "SELECT id FROM " + tableName + " WHERE int_part >= 10"))
                .matches("VALUES 1, 2");
        assertThat(query(session, "SELECT id FROM " + tableName + " WHERE int_part IN (10, 20)"))
                .matches("VALUES 1, 2");
        assertThat(query("SELECT id FROM " + tableName + " WHERE int_part IS NULL AND string_part IS NULL"))
                .matches("VALUES 3");
        assertThat(query("SELECT id FROM " + tableName + " WHERE int_part IS NOT NULL AND string_part IS NOT NULL"))
                .matches("VALUES 1, 2");

        assertThat(query("SELECT id FROM " + tableName + " WHERE int_part = 10 AND string_part = 'unmatched partition condition'"))
                .returnsEmptyResult();
        assertThat(query("SELECT id FROM " + tableName + " WHERE int_part IS NULL AND string_part IS NOT NULL"))
                .returnsEmptyResult();
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
