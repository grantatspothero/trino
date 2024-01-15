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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.FunctionId;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.spi.Plugin;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.AbstractThrowableAssert;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.objectstore.MinioStorage.ACCESS_KEY;
import static io.trino.plugin.objectstore.MinioStorage.SECRET_KEY;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_FIELD;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_OR_REPLACE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_TYPE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_SET_COLUMN_TYPE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class BaseObjectStoreConnectorTest
        extends BaseConnectorTest
{
    protected final String bucketName = "test-bucket-" + randomNameSuffix();
    private final TableType tableType;
    private MinioStorage minio;
    private TestingGalaxyMetastore metastore;

    protected BaseObjectStoreConnectorTest(TableType tableType)
    {
        this.tableType = requireNonNull(tableType, "tableType is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(
                tableType,
                Map.of(),
                Map.of());
    }

    protected final QueryRunner createQueryRunner(
            TableType tableType,
            Map<String, String> coordinatorProperties,
            Map<String, String> extraObjectStoreProperties)
            throws Exception
    {
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());
        minio = closeAfterClass(new MinioStorage(bucketName));
        minio.start();

        metastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> !location.contains("denied")));
        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> galaxyCockroachContainer));
        TestingAccountClient accountClient = testingAccountFactory.createAccountClient();

        Map<String, String> objectStoreProperties = ImmutableMap.<String, String>builder()
                .putAll(extraObjectStoreProperties)
                .put("HIVE__hive.partition-projection-enabled", "true")
                .buildOrThrow();

        DistributedQueryRunner queryRunner = ObjectStoreQueryRunner.builder()
                .withTableType(tableType)
                .withAccountClient(accountClient)
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getHiveS3Config())
                .withMetastore(metastore)
                .withLocationSecurityServer(locationSecurityServer)
                .withMockConnectorPlugin(buildMockConnectorPlugin())
                .withCoordinatorProperties(coordinatorProperties)
                .withExtraObjectStoreProperties(objectStoreProperties)
                .withPlugin(getObjectStorePlugin())
                .build();

        initializeTpchTables(queryRunner, metastore);

        // Grant select on mock catalog
        queryRunner.execute(format("GRANT SELECT ON \"mock_dynamic_listing\".\"*\".\"*\" TO ROLE %s WITH GRANT OPTION", ACCOUNT_ADMIN));

        // Grant execute on unload function
        CatalogId catalogId = accountClient.getOrCreateCatalog(queryRunner.getDefaultSession().getCatalog().orElseThrow());
        FunctionId functionId = new FunctionId(catalogId, "system", "unload");
        accountClient.grantFunctionPrivilege(new TestingAccountClient.GrantDetails(Privilege.EXECUTE, accountClient.getAdminRoleId(), GrantKind.ALLOW, false, functionId));

        return queryRunner;
    }

    protected Plugin getObjectStorePlugin()
    {
        return new ObjectStorePlugin();
    }

    protected void initializeTpchTables(DistributedQueryRunner queryRunner, TestingGalaxyMetastore metastore)
    {
        ObjectStoreQueryRunner.initializeTpchTables(queryRunner, REQUIRED_TPCH_TABLES);
    }

    // mock catalog init inside create query runner to assign catalog ID to it
    @Override
    public void initMockCatalog() {}

    @Override
    protected abstract boolean hasBehavior(TestingConnectorBehavior connectorBehavior);

    // TODO why we need this? Object Store allows only auto-commit, so the test should pass
    @Override
    protected void assertWriteNotAllowedInTransaction(TestingConnectorBehavior behavior, String sql)
    {
        // skip test since we only support auto-commit
    }

    @Test
    @Override // Override because the error message is different from "This connector does not support creating functions"
    public void testCreateFunction()
    {
        assertThatThrownBy(super::testCreateFunction)
                .hasMessageContaining("Access Denied: Cannot create function");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return abort("Connector does not support column default values");
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
        return objectStoreTestMaxTableNameLength();
    }

    public static OptionalInt objectStoreTestMaxTableNameLength()
    {
        // Limit table name length for MinIO and Galaxy metastore HTTP API
        // (used to be 255 - UUID.randomUUID().toString().length() but minio/minio:RELEASE.2022-10-05T14-58-27Z tightened limit)
        return OptionalInt.of(240 - UUID.randomUUID().toString().length());
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return objectStoreTestMaxSchemaNameLength();
    }

    public static OptionalInt objectStoreTestMaxSchemaNameLength()
    {
        // Limit schema name length for MinIO and Galaxy metastore HTTP API
        // (used to be 255 - UUID.randomUUID().toString().length() but minio/minio:RELEASE.2022-10-05T14-58-27Z tightened limit)
        return OptionalInt.of(240 - UUID.randomUUID().toString().length());
    }

    // Override and disable the negative tests for long schema and table names, because galaxy metastore has no problems storing them
    @Test
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

    @Test
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

    @Test
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
        assertThat(getQueryRunner().tableExists(getSession(), validTableName)).isTrue();
        assertUpdate("DROP TABLE " + validTableName);
    }

    @Test
    @Override
    public void testRenameTableToLongTableName()
    {
        // TODO (https://github.com/starburstdata/stargate/issues/9925) overridden because it's unknown what table name length would be a problem for ALTER TABLE RENAME TO
        //  currently, the test doesn't test failure when name is too long and this should be fixed

        skipTestUnless(hasBehavior(SUPPORTS_RENAME_TABLE));

        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);

        String baseTableName = "test_rename_target_" + randomNameSuffix();

        int maxLength = maxTableNameLength().orElseThrow();

        String validTargetTableName = baseTableName + "z".repeat(maxLength - baseTableName.length());
        assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO " + validTargetTableName);
        assertThat(getQueryRunner().tableExists(getSession(), validTargetTableName)).isTrue();
        assertQuery("SELECT x FROM " + validTargetTableName, "VALUES 123");
        assertUpdate("DROP TABLE " + validTargetTableName);
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        // Limit column name length for MinIO and Galaxy metastore HTTP API
        return OptionalInt.of(255 - UUID.randomUUID().toString().length());
    }

    @Test
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
        assertThat(columnExists(tableName, validColumnName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
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
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertQuery("SELECT x FROM " + tableName, "VALUES 123");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
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
    public void testDropSchemaCascadeWithAllType()
    {
        String schemaName = "test_drop_schema_cascade" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

        assertUpdate("CREATE TABLE " + schemaName + ".test_hive(a int) WITH (type = 'HIVE')");
        assertUpdate("CREATE TABLE " + schemaName + ".test_iceberg(a int) WITH (type = 'ICEBERG')");
        assertUpdate("CREATE TABLE " + schemaName + ".test_delta(a int) WITH (type = 'DELTA')");
        assertQueryFails("CREATE TABLE " + schemaName + ".test_hudi(a int) WITH (type = 'HUDI')", "Table creation is not supported for Hudi");
        assertThat(computeActual("SHOW TABLES IN " + schemaName).getOnlyColumnAsSet())
                .contains("test_hive", "test_iceberg", "test_delta");
        assertThat(minio.listObjects(schemaName)).isNotEmpty();

        assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        assertThat(minio.listObjects(schemaName)).isEmpty();
    }

    @Test
    public void testDropSchemaCascadeFailure()
            throws Exception
    {
        String schemaName = "test_drop_schema_cascade_failure" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

        // Create a table with system table name to cause query failure during dropping schema
        assertUpdate("CREATE TABLE " + schemaName + ".\"test_system_table$partitions\"(a int) WITH (type = 'HIVE')");
        assertUpdate("CREATE VIEW " + schemaName + ".test_view AS SELECT 1 a");
        assertUpdate("CREATE MATERIALIZED VIEW " + schemaName + ".test_materialized_view AS SELECT 1 a");

        assertQueryFails("DROP SCHEMA " + schemaName + " CASCADE", "Unexpected table present( in Hive metastore)?: %s.test_system_table\\$partitions".formatted(schemaName));
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);
        assertThat(computeActual("SHOW TABLES IN " + schemaName).getOnlyColumnAsSet())
                .contains("test_system_table$partitions");

        metastore.getMetastore().dropTable(schemaName, "test_system_table$partitions");
        metastore.getMetastore().dropDatabase(schemaName);
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        assertThat(computeScalar("SHOW CREATE SCHEMA tpch")).isEqualTo("" +
                "CREATE SCHEMA objectstore.tpch\n" +
                format("AUTHORIZATION ROLE %s\n", ACCOUNT_ADMIN) +
                "WITH (\n" +
                "   location = 's3://" + bucketName + "/tpch'\n" +
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
        // Use same table name in all assert*TableFormat methods to verify potential metastore caching doesn't affect table loading after table type changed
        @Language("SQL") String createTable = format("" +
                "CREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = '%s',\n" +
                "   type = 'HIVE'\n" +
                ")", format);

        assertUpdate(createTable);

        assertThat(computeScalar("SHOW CREATE TABLE test_type_format")).isEqualTo(createTable);

        assertUpdate("DROP TABLE test_type_format");
    }

    private void assertIcebergTableFormat(String format)
    {
        // Use same table name in all assert*TableFormat methods to verify potential metastore caching doesn't affect table loading after table type changed
        assertUpdate(format("" +
                "CREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = '%s',\n" +
                "   type = 'ICEBERG'\n" +
                ")", format));

        assertThat((String) computeActual("SHOW CREATE TABLE test_type_format").getOnlyValue()).matches("" +
                "\\QCREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = '" + format + "',\n" +
                "   format_version = 2,\n" +
                "   location = 's3://test-bucket-\\E\\w+\\Q/tpch/test_type_format\\E" + locationUuidRegex() + "\\Q',\n" +
                "   type = 'ICEBERG'\n" +
                ")\\E");

        assertUpdate("DROP TABLE test_type_format");
    }

    private void assertDeltaTableFormat()
    {
        // Use same table name in all assert*TableFormat methods to verify potential metastore caching doesn't affect table loading after table type changed
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
                "   location = 's3://test-bucket-\\E\\w+\\Q/tpch/test_type_format\\E" + locationUuidRegex() + "\\Q',\n" +
                "   type = 'DELTA'\n" +
                ")\\E");

        assertUpdate("DROP TABLE test_type_format");
    }

    private void assertHudiTableFormat()
    {
        // Use same table name in all assert*TableFormat methods to verify potential metastore caching doesn't affect table loading after table type changed
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

        assertThat(computeScalar("SHOW CREATE TABLE test_hive_specific_property")).isEqualTo("" +
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
    public void testHiveSpecificColumnProperty()
    {
        assertUpdate("" +
                "CREATE TABLE test_hive_specific_column_property(\n" +
                "   xyz bigint,\n" +
                "   abc bigint\n" +
                "     WITH (partition_projection_type = 'INTEGER', partition_projection_range = ARRAY['0', '10'])\n" +
                ") WITH (" +
                "    partitioned_by = ARRAY['abc'],\n" +
                "    partition_projection_enabled = true,\n" +
                "    partition_projection_location_template = 's3://example/${abc}'\n" +
                ")");

        assertThat(computeScalar("SHOW CREATE TABLE test_hive_specific_column_property")).isEqualTo("" +
                "CREATE TABLE objectstore.tpch.test_hive_specific_column_property (\n" +
                "   xyz bigint,\n" +
                "   abc bigint WITH ( partition_projection_range = ARRAY['0','10'], partition_projection_type = 'INTEGER' )\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC',\n" +
                "   partition_projection_enabled = true,\n" +
                "   partition_projection_location_template = 's3://example/${abc}',\n" +
                "   partitioned_by = ARRAY['abc'],\n" +
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
                "   format = 'PARQUET',\n" +
                "   format_version = 2,\n" +
                "   location = 's3://test-bucket-\\E\\w+\\Q/tpch/test_iceberg_specific_property\\E" + locationUuidRegex() + "\\Q',\n" +
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
                "   location = 's3://test-bucket-\\E\\w+\\Q/tpch/test_delta_specific_property\\E" + locationUuidRegex() + "\\Q',\n" +
                "   type = 'DELTA'\n" +
                ")\\E");
    }

    @Test
    public void testCreateOrReplaceTableDoesNotChangeTableType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_OR_REPLACE_TABLE));
        assertUpdate("CREATE OR REPLACE TABLE test_create_or_replace_table_type AS SELECT 1 AS abc", 1);
        switch (tableType) {
            case HIVE, HUDI -> fail(format("CREATE OR REPLACE TABLE queries are not supporred for the table type: %s", tableType));
            case ICEBERG -> {
                assertQueryFails(
                        "CREATE OR REPLACE TABLE test_create_or_replace_table_type (abc integer) WITH (type = 'DELTA')",
                        "tpch.test_create_or_replace_table_type is not a Delta Lake table");
                assertQueryFails(
                        "CREATE OR REPLACE TABLE test_create_or_replace_table_type WITH (type = 'DELTA') AS SELECT 2 AS abc",
                        "tpch.test_create_or_replace_table_type is not a Delta Lake table");
            }
            case DELTA -> {
                assertQueryFails(
                        "CREATE OR REPLACE TABLE test_create_or_replace_table_type (abc integer) WITH (type = 'ICEBERG')",
                        "Not an Iceberg table: tpch.test_create_or_replace_table_type");
                assertQueryFails(
                        "CREATE OR REPLACE TABLE test_create_or_replace_table_type WITH (type = 'ICEBERG') AS SELECT 2 AS abc",
                        "Not an Iceberg table: tpch.test_create_or_replace_table_type");
            }
        }
        assertQuery("SELECT * FROM test_create_or_replace_table_type", "VALUES 1");
    }

    @Test
    public void testMigrateToIcebergTable()
    {
        assertUpdate("CREATE TABLE test_migrate_to_iceberg AS SELECT 1 AS abc", 1);
        switch (tableType) {
            case HIVE -> {
                assertUpdate("ALTER TABLE test_migrate_to_iceberg SET PROPERTIES type = 'ICEBERG'");
                assertThat((String) computeScalar("SHOW CREATE TABLE test_migrate_to_iceberg"))
                        .contains("type = 'ICEBERG'");
                assertQuery("SELECT * FROM test_migrate_to_iceberg", "VALUES 1");
            }
            case ICEBERG, DELTA, HUDI -> assertQueryFails(
                    "ALTER TABLE test_migrate_to_iceberg SET PROPERTIES type = 'ICEBERG'",
                    "Changing table type from '%s' to 'ICEBERG' is not supported".formatted(tableType));
        }
    }

    @Test
    public void testMigrateToDeltaTable()
    {
        assertUpdate("CREATE TABLE test_migrate_to_delta AS SELECT 1 AS abc", 1);
        assertQueryFails(
                "ALTER TABLE test_migrate_to_delta SET PROPERTIES type = 'DELTA'",
                "Changing table type from '%s' to 'DELTA' is not supported".formatted(tableType));
    }

    @Test
    public void testMigrateToHiveTable()
    {
        assertUpdate("CREATE TABLE test_migrate_to_hive AS SELECT 1 AS abc", 1);
        assertQueryFails(
                "ALTER TABLE test_migrate_to_hive SET PROPERTIES type = 'HIVE'",
                "Changing table type from '%s' to 'HIVE' is not supported".formatted(tableType));
    }

    @Test
    public void testMigrateToHudiTable()
    {
        assertUpdate("CREATE TABLE test_migrate_to_hudi AS SELECT 1 AS abc", 1);
        assertQueryFails(
                "ALTER TABLE test_migrate_to_hudi SET PROPERTIES type = 'HUDI'",
                "Changing table type from '%s' to 'HUDI' is not supported".formatted(tableType));
    }

    @Test
    public void testCreateSchemaWithLocation()
    {
        String location = "s3://%s/denied".formatted(bucketName);
        assertQueryFails("CREATE SCHEMA test_location_create WITH (location = '" + location + "')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);
    }

    @Test
    public void testCreateTableWithLocation()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        String location = "s3://%s/denied".formatted(bucketName);

        assertQueryFails("CREATE TABLE test_location_create (x int) WITH (location = '" + location + "')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);

        assertQueryFails("CREATE TABLE test_location_create (x int) WITH (location = '" + location + "/test_location_create')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location + "/test_location_create");
    }

    @Test
    public void testCreateTableAsWithLocation()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        String location = "s3://%s/denied".formatted(bucketName);

        assertQueryFails("CREATE TABLE test_location_ctas WITH (location = '" + location + "') AS SELECT 123 x",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);

        assertQueryFails("CREATE TABLE test_location_ctas WITH (location = '" + location + "/test_location_ctas') AS SELECT 123 x",
                "Access Denied: Role accountadmin is not allowed to use location: " + location + "/test_location_ctas");
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
                        "('objectstore', 'mode', 'INCREMENTAL', 'varchar', 'Analyze mode. Possible values: [INCREMENTAL, FULL_REFRESH]'), " +
                        "('objectstore', 'partitions', '', 'array(array(varchar))', 'Partitions to be analyzed'), " +
                        "('objectstore', 'columns', '', 'array(varchar)', 'Columns to be analyzed'), " +
                        "('objectstore', 'files_modified_after', '' , 'timestamp(3) with time zone', 'Take into account only files modified after given timestamp') ");
    }

    @Test
    public void testRegisterTableProcedure()
            throws Exception
    {
        String tableName = "test_register_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x", 1);

        String tableLocation = getTableLocation(tableName);
        metastore.getMetastore().dropTable("tpch", tableName);
        if (tableType != TableType.ICEBERG) {
            // Table existence can be cached by the connector, unless we delegate to IcebergMetadata first, which currently does cache between queries.
            assertUpdate("CALL system.flush_metadata_cache(SCHEMA_NAME => CURRENT_SCHEMA, TABLE_NAME => '" + tableName + "')");
        }

        assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");

        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableProcedureIcebergSpecificArgument()
            throws Exception
    {
        String tableName = "test_register_table_iceberg_specific_argument_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x", 1);

        String tableLocation = getTableLocation(tableName);
        metastore.getMetastore().dropTable("tpch", tableName);
        if (tableType != TableType.ICEBERG) {
            // Table existence can be cached by the connector, unless we delegate to IcebergMetadata first, which currently does cache between queries.
            assertUpdate("CALL system.flush_metadata_cache(SCHEMA_NAME => CURRENT_SCHEMA, TABLE_NAME => '" + tableName + "')");
        }

        switch (tableType) {
            case ICEBERG -> {
                String key = tableLocation.substring(minio.getS3Url().length()) + "/metadata/";
                String metadataFileName = minio.listObjects(key).stream()
                        .filter(path -> path.endsWith(".json"))
                        .map(path -> Path.of(path).getFileName().toString())
                        .max(Comparator.comparing((String fileName) -> {
                            // e.g. "00001-dd701085-154b-4ca3-af16-5a4359ffbdf5.metadata.json"
                            Matcher matcher = Pattern.compile("(\\d{5})(-[0-9a-f]+){5}\\.metadata\\.json").matcher(fileName);
                            verify(matcher.matches(), "no match for [%s] in [%s]", matcher.pattern().pattern(), fileName);
                            return parseInt(matcher.group(1));
                        }))
                        .orElseThrow();
                assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', '" + metadataFileName + "')");
                assertQuery("SELECT * FROM " + tableName, "VALUES 1");
            }
            case DELTA -> {
                assertQueryFails(
                        "CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', 'dummy metadata_file_name argument')",
                        "Unsupported metadata_file_name argument.*");
                assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");
            }
            case HUDI, HIVE -> {
                assertQueryFails(
                        "CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', 'dummy metadata_file_name argument')",
                        "Unsupported table type");
                assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");
            }
        }

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testRegisterTableTypesFailure()
    {
        String tableName = "test_register_table_types_" + randomNameSuffix();

        String tableLocation = "s3://%s/%s".formatted(bucketName, tableName);

        minio.putObject(tableName + "/metadata/dummy_metadata.json", "dummy");
        minio.putObject(tableName + "/_delta_log/dummy_transaction.json", "dummy");
        minio.putObject(tableName + "/.hoodie/dummy.commit", "dummy");

        assertQueryFails(
                "CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')",
                "Cannot determine any one of Iceberg, Delta Lake, Hudi table types");
        assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");
    }

    @Test
    public void testRegisterTableAccessControl()
    {
        String tableName = "test_register_table_" + randomNameSuffix();
        String location = "s3://%s/denied".formatted(bucketName);
        assertQueryFails("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + location + "')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);
    }

    @Test
    public void testUnregisterTableProcedure()
    {
        String tableName = "test_unregister_table_" + randomNameSuffix();
        String unregisterTableName = tableName + "_new";

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x", 1);

        String tableLocation = getTableLocation(tableName);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '" + unregisterTableName + "', '" + tableLocation + "')");
        assertThat(getQueryRunner().tableExists(getSession(), unregisterTableName)).isTrue();

        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + unregisterTableName + "')");
        assertThat(getQueryRunner().tableExists(getSession(), unregisterTableName)).isFalse();

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnregisterTableAccessControl()
    {
        String tableName = "test_unregister_table_access_control_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);

        assertAccessDenied(
                "CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')",
                "Cannot drop table .*",
                privilege(tableName, DROP_TABLE));

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropTableCorruptStorage()
    {
        String tableName = "corrupt_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (name VARCHAR(256), age INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('Joe', 30)", 1);

        String tableLocation = getTableLocation(tableName);
        String tableLocationKey = tableLocation.replaceFirst(minio.getS3Url(), "");

        // break the table by deleting all its files including metadata files
        List<String> keys = minio.listObjects(tableLocationKey);
        minio.deleteObjects(keys);

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    protected String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher matcher = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (matcher.find()) {
            String location = matcher.group(1);
            verify(!matcher.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    @Test
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
                              "Setting column type on .{4,10} tables is not supported"
                            to match regex:
                              "This connector does not support setting column types"
                            but did not.
                            (?s:.*)""");
        }
    }

    @Test
    @Override // ObjectStore supports this partially, so has non-standard error message
    public void testAddRowField()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ROW_TYPE));

        if (hasBehavior(SUPPORTS_ADD_FIELD)) {
            super.testAddRowField();
        }
        else {
            assertThatThrownBy(super::testAddRowField)
                    .hasMessageMatching("""

                            Expecting message:
                              "Adding fields to .{4,10} tables is not supported"
                            to match regex:
                              "This connector does not support adding fields"
                            but did not.
                            (?s:.*)""");
        }
    }

    @Test
    public void testFlushMetadataCache()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        assertUpdate("CREATE TABLE test_flush_metadata_cache(a integer)");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_flush_metadata_cache')");
        assertUpdate("DROP TABLE test_flush_metadata_cache");

        assertUpdate("CALL system.flush_metadata_cache(schema_name => 'flush_metadata_cache_bogus_schema', table_name => 'flush_metadata_cache_non_existent')");
    }

    @Test
    @Override
    public void testCreateViewSchemaNotFound()
    {
        if (!hasBehavior(SUPPORTS_CREATE_VIEW)) {
            super.testCreateViewSchemaNotFound();
            return;
        }

        // GalaxyAccessControl.checkCanCreateView maybe should throw "Schema xxxx not found"?
        assertThatThrownBy(super::testCreateViewSchemaNotFound)
                .isInstanceOf(AssertionError.class)
                .hasMessageFindingMatch("""

                        Expecting message:
                          "Access Denied: Cannot create view objectstore.test_schema_\\S*.test_view_create_no_schema_\\S*: Role accountadmin does not have the privilege CREATE_TABLE on the schema objectstore.test_schema_\\S*"
                        to match regex:
                          "Schema test_schema_\\S* not found"
                        but did not.
                        """)
                .hasStackTraceContaining("at io.trino.server.security.galaxy.GalaxyAccessControl.checkCanCreateView");
    }

    @Test
    @Override
    public void testCreateTableSchemaNotFound()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            super.testCreateTableAsSelectSchemaNotFound();
            return;
        }

        // GalaxyAccessControl.checkCanCreateTable maybe should throw "Schema xxxx not found"?
        assertThatThrownBy(super::testCreateTableSchemaNotFound)
                .isInstanceOf(AssertionError.class)
                .hasMessageFindingMatch("""

                        Expecting message:
                          "Access Denied: Cannot create table objectstore.test_schema_\\S*.test_create_no_schema_\\S*: Role accountadmin does not have the privilege CREATE_TABLE on the schema objectstore.test_schema_\\S*"
                        to match regex:
                          "Schema test_schema_\\S* not found"
                        but did not.
                        """)
                .hasStackTraceContaining("at io.trino.server.security.galaxy.GalaxyAccessControl.checkCanCreateTable");
    }

    @Test
    @Override
    public void testCreateTableAsSelectSchemaNotFound()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            super.testCreateTableAsSelectSchemaNotFound();
            return;
        }

        // GalaxyAccessControl.checkCanCreateTable maybe should throw "Schema xxxx not found"?
        assertThatThrownBy(super::testCreateTableAsSelectSchemaNotFound)
                .isInstanceOf(AssertionError.class)
                .hasMessageFindingMatch("""

                        Expecting message:
                          "Access Denied: Cannot create table objectstore.test_schema_\\S*.test_ctas_no_schema_\\S*: Role accountadmin does not have the privilege CREATE_TABLE on the schema objectstore.test_schema_\\S*"
                        to match regex:
                          "Schema test_schema_\\S* not found"
                        but did not.
                        """)
                .hasStackTraceContaining("at io.trino.server.security.galaxy.GalaxyAccessControl.checkCanCreateTable");
    }

    @Test
    @Override
    public void testSelectInTransaction()
    {
        // Select in transaction not supported in galaxy
    }

    @Test
    public void testUnloadTableFunction()
    {
        String tableName = "test_unload" + randomNameSuffix();
        String location = "s3://%s/%s".formatted(bucketName, tableName);

        MaterializedResult result = computeActual("SELECT * FROM TABLE(system.unload(" +
                "input => TABLE(VALUES 'test unload') t(col)," +
                "location => '" + location + "'," +
                "format => 'TEXTFILE'," +
                "compression => 'GZIP'," +
                "separator => '#'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((String) getOnlyElement(result.getMaterializedRows()).getField(0)).startsWith(location);

        assertUpdate("CREATE TABLE " + tableName + "(col varchar)" +
                "WITH (type = 'HIVE', format = 'TEXTFILE', textfile_field_separator = '#', external_location = '" + location + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES 'test unload'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnloadTableFunctionLocationDenied()
    {
        String tableName = "test_unload_denied" + randomNameSuffix();
        String location = "s3://%s/%s".formatted(bucketName, tableName);

        assertQueryFails(
                "SELECT * FROM TABLE(system.unload(" +
                "input => TABLE(VALUES 'test unload denied') t(col)," +
                "location => '" + location + "'," +
                "format => 'TEXTFILE'))",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);
    }

    @Override
    protected void verifyRefreshMaterializedViewFailureWithoutMultiWriteInTransactionSupport(AbstractThrowableAssert abstractThrowableAssert)
    {
        abstractThrowableAssert.hasMessageContaining("Catalogs already associated with transaction");
    }

    protected TrinoFileSystem getTrinoFileSystem()
    {
        ConfigurationInitializer s3Initializer = new TrinoS3ConfigurationInitializer(new HiveS3Config()
                .setS3AwsAccessKey(ACCESS_KEY)
                .setS3AwsSecretKey(SECRET_KEY));
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(s3Initializer));
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, ImmutableSet.of());
        return new HdfsFileSystemFactory(new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication()), new TrinoHdfsFileSystemStats())
                .create(SESSION);
    }

    @Override
    protected Session withoutSmallFileThreshold(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_small_file_threshold", "0B")
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "orc_tiny_stripe_threshold", "0B")
                .build();
    }
}
