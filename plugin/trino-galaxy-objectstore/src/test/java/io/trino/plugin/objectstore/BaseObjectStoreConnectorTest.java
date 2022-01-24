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

import io.trino.hdfs.TrinoFileSystemCache;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_SET_COLUMN_TYPE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public abstract class BaseObjectStoreConnectorTest
        extends BaseConnectorTest
{
    private final TableType tableType;

    protected BaseObjectStoreConnectorTest(TableType tableType)
    {
        this.tableType = requireNonNull(tableType, "tableType is null");
    }

    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        closeAfterClass(TrinoFileSystemCache.INSTANCE::closeAll);

        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());
        MinioStorage minio = closeAfterClass(new MinioStorage("test-bucket"));
        minio.start();

        TestingGalaxyMetastore metastore = closeAfterClass(new TestingGalaxyMetastore(Optional.of(galaxyCockroachContainer)));

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> false));

        DistributedQueryRunner queryRunner = ObjectStoreQueryRunner.builder()
                .withTableType(tableType)
                .withCockroach(galaxyCockroachContainer)
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getHiveS3Config())
                .withMetastore(metastore)
                .withLocationSecurityServer(locationSecurityServer)
                .withMockConnectorPlugin(buildMockConnectorPlugin())
                .build();

        initializeTpchTables(queryRunner, metastore);

        // Grant select on mock catalog
        queryRunner.execute(format("GRANT SELECT ON \"mock_dynamic_listing\".\"*\".\"*\" TO ROLE %s WITH GRANT OPTION", ACCOUNT_ADMIN));
        return queryRunner;
    }

    protected void initializeTpchTables(DistributedQueryRunner queryRunner, TestingGalaxyMetastore metastore)
    {
        ObjectStoreQueryRunner.initializeTpchTables(queryRunner, REQUIRED_TPCH_TABLES);
    }

    // mock catalog init inside create query runner to assign catalog ID to it
    @Override
    public void initMockCatalog() {}

    @Override
    @SuppressWarnings("DuplicateBranchesInSwitch")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return true;

            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_RENAME_MATERIALIZED_VIEW:
                return true;
            case SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    // TODO why we need this? Object Store allows only auto-commit, so the test should pass
    @Override
    protected void assertWriteNotAllowedInTransaction(TestingConnectorBehavior behavior, String sql)
    {
        // skip test since we only support auto-commit
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Connector does not support column default values");
    }

    @Override
    protected void verifyVersionedQueryFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageMatching("" +
                "This connector does not support versioned tables .*|" +
                "Versioning is only supported for Iceberg tables");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        // Limit table name length for MinIO and Galaxy metastore HTTP API
        // (used to be 255 - UUID.randomUUID().toString().length() but minio/minio:RELEASE.2022-10-05T14-58-27Z tightened limit)
        return OptionalInt.of(240 - UUID.randomUUID().toString().length());
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        // Limit schema name length for MinIO and Galaxy metastore HTTP API
        // (used to be 255 - UUID.randomUUID().toString().length() but minio/minio:RELEASE.2022-10-05T14-58-27Z tightened limit)
        return OptionalInt.of(240 - UUID.randomUUID().toString().length());
    }

    // Override and disable the negative tests for long schema and table names, because galaxy metastore has no problems storing them
    @Override
    public void testCreateSchemaWithLongName()
    {
        String baseSchemaName = "test_create_" + randomNameSuffix();

        int maxLength = maxSchemaNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validSchemaName = baseSchemaName + "z".repeat(maxLength - baseSchemaName.length());
        assertUpdate("CREATE SCHEMA " + validSchemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(validSchemaName);
        assertUpdate("DROP SCHEMA " + validSchemaName);
    }

    @Override
    public void testRenameSchemaToLongName()
    {
        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + sourceTableName);

        String baseSchemaName = "test_rename_target_" + randomNameSuffix();

        int maxLength = maxSchemaNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetSchemaName = baseSchemaName + "z".repeat(maxLength - baseSchemaName.length());
        assertUpdate("ALTER SCHEMA " + sourceTableName + " RENAME TO " + validTargetSchemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(validTargetSchemaName);
        assertUpdate("DROP SCHEMA " + validTargetSchemaName);
    }

    @Override
    public void testCreateTableWithLongTableName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String baseTableName = "test_create_" + randomNameSuffix();

        int maxLength = maxTableNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTableName = baseTableName + "z".repeat(maxLength - baseTableName.length());
        assertUpdate("CREATE TABLE " + validTableName + " (a bigint)");
        assertTrue(getQueryRunner().tableExists(getSession(), validTableName));
        assertUpdate("DROP TABLE " + validTableName);
    }

    @Override
    public void testRenameTableToLongTableName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_RENAME_TABLE));

        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);

        String baseTableName = "test_rename_target_" + randomNameSuffix();

        int maxLength = maxTableNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetTableName = baseTableName + "z".repeat(maxLength - baseTableName.length());
        assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO " + validTargetTableName);
        assertTrue(getQueryRunner().tableExists(getSession(), validTargetTableName));
        assertQuery("SELECT x FROM " + validTargetTableName, "VALUES 123");
        assertUpdate("DROP TABLE " + validTargetTableName);
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        // Limit column name length for MinIO and Galaxy metastore HTTP API
        return OptionalInt.of(255 - UUID.randomUUID().toString().length());
    }

    @Override
    public void testCreateTableWithLongColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName = "test_long_column" + randomNameSuffix();
        String basColumnName = "col";

        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validColumnName = basColumnName + "z".repeat(maxLength - basColumnName.length());
        assertUpdate("CREATE TABLE " + tableName + " (" + validColumnName + " bigint)");
        assertTrue(columnExists(tableName, validColumnName));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testAlterTableAddLongColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_ADD_COLUMN));

        String tableName = "test_long_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String basColumnName = "col";
        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetColumnName = basColumnName + "z".repeat(maxLength - basColumnName.length());
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + validTargetColumnName + " int");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertQuery("SELECT x FROM " + tableName, "VALUES 123");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_RENAME_COLUMN));

        String tableName = "test_long_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String baseColumnName = "col";
        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO " + validTargetColumnName);
        assertQuery("SELECT " + validTargetColumnName + " FROM " + tableName, "VALUES 123");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        assertQueryReturns("SHOW CREATE SCHEMA tpch", "" +
                "CREATE SCHEMA objectstore.tpch\n" +
                format("AUTHORIZATION ROLE %s\n", ACCOUNT_ADMIN) +
                "WITH (\n" +
                "   location = 's3://test-bucket/tpch'\n" +
                ")");
    }

    @Test
    @Override
    public void testShowCreateInformationSchema()
    {
        assertThat(computeScalar("SHOW CREATE SCHEMA information_schema"))
                .isEqualTo(format("CREATE SCHEMA %s.information_schema\nAUTHORIZATION ROLE %s", getSession().getCatalog().orElseThrow(), ACCOUNT_ADMIN));
    }

    @Test
    public void testTableTypesAndFormats()
    {
        assertHiveTableFormat("PARQUET");
        assertHiveTableFormat("ORC");
        assertHiveTableFormat("TEXTFILE");

        assertIcebergTableFormat("PARQUET");
        assertIcebergTableFormat("ORC");

        assertDeltaTableFormat();

        assertHudiTableFormat();
    }

    private static String locationUuidRegex()
    {
        return "-[0-9a-f]{32}";
    }

    private void assertHiveTableFormat(String format)
    {
        @Language("SQL") String createTable = format("" +
                "CREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = '%s',\n" +
                "   type = 'HIVE'\n" +
                ")", format);

        assertUpdate(createTable);

        assertQueryReturns("SHOW CREATE TABLE test_type_format", createTable);

        assertUpdate("DROP TABLE test_type_format");
    }

    private void assertIcebergTableFormat(String format)
    {
        assertUpdate(format("" +
                "CREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = '%s',\n" +
                "   type = 'ICEBERG'\n" +
                ")", format));

        assertThat((String) computeActual("SHOW CREATE TABLE test_type_format").getOnlyValue()).matches(format("" +
                "\\QCREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = '%s',\n" +
                "   format_version = 2,\n" +
                "   location = 's3://test-bucket/tpch/test_type_format\\E" + locationUuidRegex() + "\\Q',\n" +
                "   type = 'ICEBERG'\n" +
                ")\\E", format));

        assertUpdate("DROP TABLE test_type_format");
    }

    private void assertDeltaTableFormat()
    {
        assertUpdate(format("" +
                "CREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   type = 'DELTA'\n" +
                ")"));

        assertThat((String) computeActual("SHOW CREATE TABLE test_type_format").getOnlyValue()).matches("" +
                "\\QCREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   location = 's3://test-bucket/tpch/test_type_format\\E" + locationUuidRegex() + "\\Q',\n" +
                "   partitioned_by = ARRAY[],\n" +
                "   type = 'DELTA'\n" +
                ")\\E");

        assertUpdate("DROP TABLE test_type_format");
    }

    private void assertHudiTableFormat()
    {
        assertQueryFails("CREATE TABLE test_type_format (abc bigint) WITH (type = 'HUDI')\n",
                "Table creation is not supported for Hudi");
    }

    @Test
    public void testHiveSpecificTableProperty()
    {
        assertUpdate("" +
                "CREATE TABLE test_hive_specific_property(\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   auto_purge = true\n" +
                ")");

        assertQueryReturns("SHOW CREATE TABLE test_hive_specific_property", "" +
                "CREATE TABLE objectstore.tpch.test_hive_specific_property (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   auto_purge = true,\n" +
                "   format = 'ORC',\n" +
                "   type = 'HIVE'\n" +
                ")");
    }

    @Test
    public void testIcebergSpecificTableProperty()
    {
        assertUpdate("" +
                "CREATE TABLE test_iceberg_specific_property(\n" +
                "   abc bigint,\n" +
                "   xyz varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   partitioning = ARRAY['bucket(abc, 13)']\n" +
                ")");

        assertThat((String) computeActual("SHOW CREATE TABLE test_iceberg_specific_property").getOnlyValue()).matches("" +
                "\\QCREATE TABLE objectstore.tpch.test_iceberg_specific_property (\n" +
                "   abc bigint,\n" +
                "   xyz varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC',\n" +
                "   format_version = 2,\n" +
                "   location = 's3://test-bucket/tpch/test_iceberg_specific_property\\E" + locationUuidRegex() + "\\Q',\n" +
                "   partitioning = ARRAY['bucket(abc, 13)'],\n" +
                "   type = 'ICEBERG'\n" +
                ")\\E");
    }

    @Test
    public void testDeltaSpecificTableProperty()
    {
        assertUpdate("" +
                "CREATE TABLE test_delta_specific_property(\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   checkpoint_interval = 42\n" +
                ")");

        assertThat((String) computeActual("SHOW CREATE TABLE test_delta_specific_property").getOnlyValue()).matches("" +
                "\\QCREATE TABLE objectstore.tpch.test_delta_specific_property (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   checkpoint_interval = 42,\n" +
                "   location = 's3://test-bucket/tpch/test_delta_specific_property\\E" + locationUuidRegex() + "\\Q',\n" +
                "   partitioned_by = ARRAY[],\n" +
                "   type = 'DELTA'\n" +
                ")\\E");
    }

    @Test
    public void testCreateSchemaWithLocation()
    {
        assertQueryFails("CREATE SCHEMA test_location_create WITH (location = 's3://test-bucket/junk')",
                "Access Denied: Role ID r-\\d{10} is not allowed to use location: s3://test-bucket/junk");
    }

    @Test
    public void testCreateTableWithLocation()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        assertQueryFails("CREATE TABLE test_location_create (x int) WITH (location = 's3://test-bucket/junk')",
                "Access Denied: Role ID r-\\d{10} is not allowed to use location: s3://test-bucket/junk");

        assertQueryFails("CREATE TABLE test_location_create (x int) WITH (location = 's3://test-bucket/tpch/test_location_create')",
                "Access Denied: Role ID r-\\d{10} is not allowed to use location: s3://test-bucket/tpch/test_location_create");
    }

    @Test
    public void testCreateTableAsWithLocation()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        assertQueryFails("CREATE TABLE test_location_ctas WITH (location = 's3://test-bucket/junk') AS SELECT 123 x",
                "Access Denied: Role ID r-\\d{10} is not allowed to use location: s3://test-bucket/junk");

        assertQueryFails("CREATE TABLE test_location_ctas WITH (location = 's3://test-bucket/tpch/test_location_ctas') AS SELECT 123 x",
                "Access Denied: Role ID r-\\d{10} is not allowed to use location: s3://test-bucket/tpch/test_location_ctas");
    }

    @Test
    public void testBasicTableStatistics()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        assertUpdate("CREATE TABLE test_basic_table_statistics (x BIGINT)");
        assertUpdate("INSERT INTO test_basic_table_statistics VALUES -42", 1);
        assertUpdate("INSERT INTO test_basic_table_statistics VALUES 88", 1);

        // SHOW STATS result: column_name, data_size, distinct_values_count, nulls_fractions, row_count, low_value, high_value

        assertThat(computeActual("SHOW STATS FOR test_basic_table_statistics"))
                .isEqualTo(resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("x", null, basicTableStatisticsExpectedNdv(2), 0.0, null, "-42", "88")
                        .row(null, null, null, null, 2.0, null, null)
                        .build());

        assertUpdate("INSERT INTO test_basic_table_statistics VALUES 222", 1);

        assertThat(computeActual("SHOW STATS FOR test_basic_table_statistics"))
                .isEqualTo(resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("x", null, basicTableStatisticsExpectedNdv(3), 0.0, null, "-42", "222")
                        .row(null, null, null, null, 3.0, null, null)
                        .build());
    }

    protected Double basicTableStatisticsExpectedNdv(int actualNdv)
    {
        return null;
    }

    @Test
    public void testAnalyzePropertiesSystemTable()
    {
        // Note, this is a union of all the analyze table properties across iceberg/delta/hive
        // for example: iceberg does not support analyze at all and only delta supports files_modified_after
        assertQuery(
                "SELECT * FROM system.metadata.analyze_properties WHERE catalog_name = 'objectstore'",
                "SELECT * FROM VALUES " +
                        "('objectstore', 'partitions', '', 'array(array(varchar))', 'Partitions to be analyzed'), " +
                        "('objectstore', 'columns', '', 'array(varchar)', 'Columns to be analyzed'), " +
                        "('objectstore', 'files_modified_after', '' , 'timestamp(3) with time zone', 'Take into account only files modified after given timestamp') ");
    }

    @Override
    public void testAddNotNullColumnToNonEmptyTable()
    {
        skipTestUnless(hasBehavior(SUPPORTS_ADD_COLUMN));

        // Override because the connector supports both ADD COLUMN and NOT NULL constraint, but it doesn't support adding NOT NULL columns
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_notnull_col", "(a_varchar varchar)")) {
            String tableName = table.getName();

            assertQueryFails(
                    "ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL",
                    ".* do not support NOT NULL columns");
        }
    }

    @Override // ObjectStore supports this partially, so has non-standard error message
    public void testSetColumnType()
    {
        if (hasBehavior(SUPPORTS_SET_COLUMN_TYPE)) {
            super.testSetColumnType();
        }
        else {
            assertThatThrownBy(super::testSetColumnType)
                    .hasMessageMatching("""

                            Expecting message:
                              <"Adding columns to .{4,10} tables is not supported">
                            to match regex:
                              <"This connector does not support setting column types">
                            but did not.
                            (?s:.*)""");
        }
    }

    // these tests are slow don't test anything specific to the connector
    @Override
    @Test(enabled = false)
    public void testLargeIn(int valuesCount) {}

    @Override
    @Test(enabled = false)
    public void testTableSampleBernoulli() {}

    @Override
    @Test(enabled = false)
    public void testTableSampleBernoulliBoundaryValues() {}

    @Override
    @Test(enabled = false)
    public void testTableSampleSystem() {}

    @Override
    @Test(enabled = false)
    public void testColumnName(String columnName) {}

    // Override here and in BaseIcebergConnectorTest
    @Override
    @Test(enabled = false)
    public void testViewAndMaterializedViewTogether() {}

    // galaxy security message overrides schema not found message
    @Override
    @Test(enabled = false)
    public void testCreateTableAsSelectSchemaNotFound() {}

    // galaxy security message overrides schema not found message
    @Override
    @Test(enabled = false)
    public void testCreateTableSchemaNotFound() {}

    // galaxy security message overrides schema not found message
    @Override
    @Test(enabled = false)
    public void testCreateViewSchemaNotFound() {}

    // increased timeout to be able to handle galaxy security RTT addition
    // TODO improve tests' speed, decrease the timeout back
    @Test(timeOut = 280_000)
    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
            throws Exception
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new SkipException("Cannot test");
        }

        int readIterations = 5;
        int testTimeoutSeconds = 260;

        testReadMetadataWithRelationsConcurrentModifications(readIterations, testTimeoutSeconds);
    }

    @Override
    protected void checkInformationSchemaViewsForMaterializedView(String schemaName, String viewName) {}

    protected void assertQueryReturns(@Language("SQL") String sql, String result)
    {
        assertThat(computeActual(sql).getOnlyValue()).isEqualTo(result);
    }
}
