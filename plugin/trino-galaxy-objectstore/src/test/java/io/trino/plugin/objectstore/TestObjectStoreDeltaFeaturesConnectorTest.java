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

import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient.GrantDetails;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.FunctionId;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.deltalake.TestDeltaLakeConnectorTest;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.objectstore.ConnectorFeaturesTestHelper.TestFramework;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.minio.MinioClient;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.BeforeAll;
import org.testng.annotations.AfterClass;
import org.junit.jupiter.api.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;
import java.lang.reflect.Method;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.CREATE_OR_REPLACE_TABLE_AS_OPERATION;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.initializeTpchTables;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests ObjectStore connector with Delta backend, exercising all
 * Delta-specific tests inherited from {@link TestDeltaLakeConnectorTest}.
 *
 * @see TestObjectStoreDeltaConnectorTest
 * @see TestObjectStoreHiveFeaturesConnectorTest
 * @see TestObjectStoreIcebergFeaturesConnectorTest
 */
public class TestObjectStoreDeltaFeaturesConnectorTest
        extends TestDeltaLakeConnectorTest
{
    private static final ConnectorFeaturesTestHelper HELPER = new ConnectorFeaturesTestHelper(TestObjectStoreDeltaFeaturesConnectorTest.class, TestObjectStoreDeltaConnectorTest.class);

    private TestingGalaxyMetastore galaxyMetastore;
    private TestFramework testFramework;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MinioStorage minio = closeAfterClass(new MinioStorage(bucketName));
        minio.start();
        minioClient = closeAfterClass(new MinioClient(minio.getEndpoint(), MinioStorage.ACCESS_KEY, MinioStorage.SECRET_KEY));

        GalaxyCockroachContainer cockroach = closeAfterClass(new GalaxyCockroachContainer());
        galaxyMetastore = closeAfterClass(new TestingGalaxyMetastore(cockroach));
        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> cockroach));
        TestingAccountClient accountClient = testingAccountFactory.createAccountClient();
        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> true));

        String catalog = DeltaLakeQueryRunner.DELTA_CATALOG;
        String schema = "test_schema"; // must match TestDeltaLakeConnectorTest.SCHEMA

        DistributedQueryRunner queryRunner = ObjectStoreQueryRunner.builder()
                .withTableType(TableType.DELTA)
                .withCatalogName(catalog)
                .withSchemaName(schema)
                .withMetastoreType("galaxy")
                .withAccountClient(accountClient)
                .withLocationSecurityServer(locationSecurityServer)
                .withMetastore(galaxyMetastore)
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getHiveS3Config())
                .build();
        try {
            // Necessary to access system tables such as $history
            queryRunner.execute("GRANT ALL PRIVILEGES ON \"%s\".\"%s\".\"*\" TO ROLE %s".formatted(catalog, schema, ACCOUNT_ADMIN));

            CatalogId catalogId = accountClient.getOrCreateCatalog(catalog);
            FunctionId functionId = new FunctionId(catalogId, "system", "table_changes");
            accountClient.grantFunctionPrivilege(new GrantDetails(Privilege.EXECUTE, accountClient.getAdminRoleId(), GrantKind.ALLOW, false, functionId));

            initializeTpchTables(queryRunner, REQUIRED_TPCH_TABLES);
            queryRunner.execute("CREATE SCHEMA schemawithoutunderscore"); // needed for testShowSchemasLikeWithEscape

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Override
    protected boolean isObjectStore()
    {
        return true;
    }

    @BeforeMethod(alwaysRun = true)
    public void preventDuplicatedTestCoverage(Method testMethod)
    {
        HELPER.preventDuplicatedTestCoverage(testMethod);
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        galaxyMetastore = null; // closed by closeAfterClass
        super.tearDown();
    }

    @BeforeClass
    public void detectTestNg()
    {
        testFramework = TestFramework.TESTNG;
    }

    @BeforeAll
    public void detectJunit()
    {
        testFramework = TestFramework.JUNIT;
    }

    @Test
    @Override
    public void ensureDistributedQueryRunner()
    {
        // duplicate test, but still desired to run
        super.ensureDistributedQueryRunner();
    }

    @Test
    @Override
    public void ensureTestNamingConvention()
    {
        // duplicate test, but still desired to run
        super.ensureTestNamingConvention();
    }

    @Test
    @Override
    // Override as the original tests depend on Hive metastore and here we use galaxy metastore
    public void testTrinoCacheInvalidatedOnCreateTable()
            throws Exception
    {
        String tableName = "test_create_table_invalidate_cache_" + randomNameSuffix();
        String tableLocation = "s3://%s/%s/%s".formatted(bucketName, SCHEMA, tableName);

        String initialValues = "VALUES" +
                " (1, BOOLEAN 'false', TINYINT '-128')" +
                ",(2, BOOLEAN 'true', TINYINT '127')" +
                ",(3, BOOLEAN 'false', TINYINT '0')" +
                ",(4, BOOLEAN 'false', TINYINT '1')" +
                ",(5, BOOLEAN 'true', TINYINT '37')";
        assertUpdate("CREATE TABLE " + tableName + "(id, boolean, tinyint) WITH (location = '" + tableLocation + "') AS " + initialValues, 5);
        assertThat(query("SELECT * FROM " + tableName)).matches(initialValues);

        galaxyMetastore.getMetastore().dropTable(SCHEMA, tableName);
        // caching metadata is enabled for ObjectStore tests, so flush it
        assertUpdate("CALL system.flush_metadata_cache(schema_name=>'" + SCHEMA + "', table_name=>'" + tableName + "')");
        for (String file : minioClient.listObjects(bucketName, SCHEMA + "/" + tableName)) {
            minioClient.removeObject(bucketName, file);
        }

        String newValues = "VALUES" +
                " (1, BOOLEAN 'true', TINYINT '1')" +
                ",(2, BOOLEAN 'true', TINYINT '1')" +
                ",(3, BOOLEAN 'false', TINYINT '2')" +
                ",(4, BOOLEAN 'true', TINYINT '3')" +
                ",(5, BOOLEAN 'true', TINYINT '5')" +
                ",(6, BOOLEAN 'false', TINYINT '8')" +
                ",(7, BOOLEAN 'true', TINYINT '13')";
        assertUpdate("CREATE TABLE " + tableName + "(id, boolean, tinyint) WITH (location = '" + tableLocation + "') AS " + newValues, 7);
        assertThat(query("SELECT * FROM " + tableName)).matches(newValues);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    // Override as the original tests depend on SHOW CREATE TABLE output specific to Objectstore
    public void testCreateOrReplaceTableChangeUnpartitionedTableIntoPartitioned()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_", " AS SELECT BIGINT '22' a, CAST('some data' AS VARCHAR) b")) {
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " WITH (partitioned_by=ARRAY['a']) AS SELECT BIGINT '42' a, 'some data' b UNION ALL SELECT BIGINT '43' a, 'another data' b", 2);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '42', CAST('some data' AS VARCHAR)), (BIGINT '43', CAST('another data' AS VARCHAR))");

            assertLatestTableOperation(table.getName(), CREATE_OR_REPLACE_TABLE_AS_OPERATION);

            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .matches("CREATE TABLE delta.test_schema.%s \\(\n".formatted(table.getName()) +
                            "   a bigint,\n" +
                            "   b varchar\n" +
                            "\\)\n" +
                            "WITH \\(\n" +
                            "   location = '.*',\n" +
                            "   partitioned_by = ARRAY\\['a'],\n" +
                            "   type = 'DELTA'\n" +
                            "\\)");
        }
    }

    @Test
    @Override
    // Override as the original tests depend on SHOW CREATE TABLE output specific to Objectstore
    public void testCreateOrReplaceTableChangePartitionedTableIntoUnpartitioned()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_create_or_replace_",
                "  WITH (partitioned_by=ARRAY['a']) AS SELECT BIGINT '42' a, 'some data' b UNION ALL SELECT BIGINT '43' a, 'another data' b")) {
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS SELECT BIGINT '42' a, 'some data' b UNION ALL SELECT BIGINT '43' a, 'another data' b", 2);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (BIGINT '42', CAST('some data' AS VARCHAR)), (BIGINT '43', CAST('another data' AS VARCHAR))");

            assertLatestTableOperation(table.getName(), CREATE_OR_REPLACE_TABLE_AS_OPERATION);

            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .matches("CREATE TABLE delta.test_schema.%s \\(\n".formatted(table.getName()) +
                            "   a bigint,\n" +
                            "   b varchar\n" +
                            "\\)\n" +
                            "WITH \\(\n" +
                            "   location = '.*',\n" +
                            "   type = 'DELTA'\n" +
                            "\\)");
        }
    }

    @Test
    @Override
    // Override as the original tests depend on Hive metastore and here we use galaxy metastore
    public void testCreateOrReplaceTableWithSameLocationForManagedTable()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "create_or_replace_with_same_location_",
                " (a BIGINT)")) {
            String location = galaxyMetastore.getMetastore().getTable("test_schema", table.getName()).orElseThrow().storage().location();
            assertTableType("test_schema", table.getName(), MANAGED_TABLE.name());
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " WITH (location = '" + location + "') AS SELECT 'abc' as colA", 1);
            assertTableType("test_schema", table.getName(), MANAGED_TABLE.name());
        }
    }

    @Override
    protected void assertTableType(String schemaName, String tableName, String tableType)
    {
        assertThat(galaxyMetastore.getMetastore().getTable(schemaName, tableName).orElseThrow().tableType()).isEqualTo(tableType);
    }

    private void skipDuplicateTestCoverage(String methodName, Class<?>... args)
    {
        HELPER.skipDuplicateTestCoverage(testFramework, methodName, args);
    }

    // Nested class because IntelliJ poorly handles case where one class is run sometimes as a test and sometimes as an application
    public static final class UpdateOverrides
    {
        private UpdateOverrides() {}

        public static void main(String[] args)
                throws IOException
        {
            HELPER.updateOverrides();
        }
    }

    /////// ----------------------------------------- please put generated code below this line ----------------------------------------- ///////
    /////// ----------------------------------------- please put generated code also below this line ------------------------------------ ///////
    /////// ----------------------------------------- please put generated code below this line as well --------------------------------- ///////

    @Test
    @Override
    public void testAddColumn()
    {
        skipDuplicateTestCoverage("testAddColumn");
    }

    @Test
    @Override
    public void testAddColumnConcurrently()
    {
        skipDuplicateTestCoverage("testAddColumnConcurrently");
    }

    @Test
    @Override
    public void testAddColumnWithComment()
    {
        skipDuplicateTestCoverage("testAddColumnWithComment");
    }

    @Test
    @Override
    public void testAddColumnWithCommentSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testAddColumnWithCommentSpecialCharacter", String.class);
    }

    @Test
    @Override
    public void testAddNotNullColumn()
    {
        skipDuplicateTestCoverage("testAddNotNullColumn");
    }

    @Test
    @Override
    public void testAddNotNullColumnToEmptyTable()
    {
        skipDuplicateTestCoverage("testAddNotNullColumnToEmptyTable");
    }

    @Test
    @Override
    public void testAddRowField()
    {
        skipDuplicateTestCoverage("testAddRowField");
    }

    @Test
    @Override
    public void testAggregation()
    {
        skipDuplicateTestCoverage("testAggregation");
    }

    @Test
    @Override
    public void testAlterTableAddLongColumnName()
    {
        skipDuplicateTestCoverage("testAlterTableAddLongColumnName");
    }

    @Test
    @Override
    public void testCaseSensitiveDataMapping(BaseConnectorTest.DataMappingTestSetup arg0)
    {
        skipDuplicateTestCoverage("testCaseSensitiveDataMapping", BaseConnectorTest.DataMappingTestSetup.class);
    }

    @Test
    @Override
    public void testColumnCommentMaterializedView()
    {
        skipDuplicateTestCoverage("testColumnCommentMaterializedView");
    }

    @Test
    @Override
    public void testColumnName(String arg0)
    {
        skipDuplicateTestCoverage("testColumnName", String.class);
    }

    @Test
    @Override
    public void testColumnsInReverseOrder()
    {
        skipDuplicateTestCoverage("testColumnsInReverseOrder");
    }

    @Test
    @Override
    public void testCommentColumn()
    {
        skipDuplicateTestCoverage("testCommentColumn");
    }

    @Test
    @Override
    public void testCommentColumnName(String arg0)
    {
        skipDuplicateTestCoverage("testCommentColumnName", String.class);
    }

    @Test
    @Override
    public void testCommentColumnSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testCommentColumnSpecialCharacter", String.class);
    }

    @Test
    @Override
    public void testCommentTable()
    {
        skipDuplicateTestCoverage("testCommentTable");
    }

    @Test
    @Override
    public void testCommentTableSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testCommentTableSpecialCharacter", String.class);
    }

    @Test
    @Override
    public void testCommentView()
    {
        skipDuplicateTestCoverage("testCommentView");
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        skipDuplicateTestCoverage("testCommentViewColumn");
    }

    @Test
    @Override
    public void testCompatibleTypeChangeForView()
    {
        skipDuplicateTestCoverage("testCompatibleTypeChangeForView");
    }

    @Test
    @Override
    public void testCompatibleTypeChangeForView2()
    {
        skipDuplicateTestCoverage("testCompatibleTypeChangeForView2");
    }

    @Test
    @Override
    public void testConcurrentScans()
    {
        skipDuplicateTestCoverage("testConcurrentScans");
    }

    @Test
    @Override
    public void testCreateFunction()
    {
        skipDuplicateTestCoverage("testCreateFunction");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableAsSelectWhenTableDoesNotExists()
    {
        skipDuplicateTestCoverage("testCreateOrReplaceTableAsSelectWhenTableDoesNotExists");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableConcurrently()
    {
        skipDuplicateTestCoverage("testCreateOrReplaceTableConcurrently");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableWhenTableAlreadyExistsSameSchema()
    {
        skipDuplicateTestCoverage("testCreateOrReplaceTableWhenTableAlreadyExistsSameSchema");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableWhenTableAlreadyExistsSameSchemaNoData()
    {
        skipDuplicateTestCoverage("testCreateOrReplaceTableWhenTableAlreadyExistsSameSchemaNoData");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableWhenTableDoesNotExist()
    {
        skipDuplicateTestCoverage("testCreateOrReplaceTableWhenTableDoesNotExist");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableWithDifferentDataType()
    {
        skipDuplicateTestCoverage("testCreateOrReplaceTableWithDifferentDataType");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableWithNewColumnNames()
    {
        skipDuplicateTestCoverage("testCreateOrReplaceTableWithNewColumnNames");
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        skipDuplicateTestCoverage("testCreateSchema");
    }

    @Test
    @Override
    public void testCreateSchemaWithLongName()
    {
        skipDuplicateTestCoverage("testCreateSchemaWithLongName");
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        skipDuplicateTestCoverage("testCreateSchemaWithNonLowercaseOwnerName");
    }

    @Test
    @Override
    public void testCreateTable()
    {
        skipDuplicateTestCoverage("testCreateTable");
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelect");
    }

    @Test
    @Override
    public void testCreateTableAsSelectNegativeDate()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectNegativeDate");
    }

    @Test
    @Override
    public void testCreateTableAsSelectSchemaNotFound()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectSchemaNotFound");
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithTableComment()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectWithTableComment");
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithTableCommentSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectWithTableCommentSpecialCharacter", String.class);
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithUnicode()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectWithUnicode");
    }

    @Test
    @Override
    public void testCreateTableSchemaNotFound()
    {
        skipDuplicateTestCoverage("testCreateTableSchemaNotFound");
    }

    @Test
    @Override
    public void testCreateTableWithColumnComment()
    {
        skipDuplicateTestCoverage("testCreateTableWithColumnComment");
    }

    @Test
    @Override
    public void testCreateTableWithColumnCommentSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testCreateTableWithColumnCommentSpecialCharacter", String.class);
    }

    @Test
    @Override
    public void testCreateTableWithLongColumnName()
    {
        skipDuplicateTestCoverage("testCreateTableWithLongColumnName");
    }

    @Test
    @Override
    public void testCreateTableWithLongTableName()
    {
        skipDuplicateTestCoverage("testCreateTableWithLongTableName");
    }

    @Test
    @Override
    public void testCreateTableWithTableComment()
    {
        skipDuplicateTestCoverage("testCreateTableWithTableComment");
    }

    @Test
    @Override
    public void testCreateTableWithTableCommentSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testCreateTableWithTableCommentSpecialCharacter", String.class);
    }

    @Test
    @Override
    public void testCreateViewSchemaNotFound()
    {
        skipDuplicateTestCoverage("testCreateViewSchemaNotFound");
    }

    @Test
    @Override
    public void testDataMappingSmokeTest(BaseConnectorTest.DataMappingTestSetup arg0)
    {
        skipDuplicateTestCoverage("testDataMappingSmokeTest", BaseConnectorTest.DataMappingTestSetup.class);
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        skipDuplicateTestCoverage("testDateYearOfEraPredicate");
    }

    @Test
    @Override
    public void testDelete()
    {
        skipDuplicateTestCoverage("testDelete");
    }

    @Test
    @Override
    public void testDeleteAllDataFromTable()
    {
        skipDuplicateTestCoverage("testDeleteAllDataFromTable");
    }

    @Test
    @Override
    public void testDeleteWithComplexPredicate()
    {
        skipDuplicateTestCoverage("testDeleteWithComplexPredicate");
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        skipDuplicateTestCoverage("testDeleteWithLike");
    }

    @Test
    @Override
    public void testDeleteWithSemiJoin()
    {
        skipDuplicateTestCoverage("testDeleteWithSemiJoin");
    }

    @Test
    @Override
    public void testDeleteWithSubquery()
    {
        skipDuplicateTestCoverage("testDeleteWithSubquery");
    }

    @Test
    @Override
    public void testDeleteWithVarcharPredicate()
    {
        skipDuplicateTestCoverage("testDeleteWithVarcharPredicate");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        skipDuplicateTestCoverage("testDescribeTable");
    }

    @Test
    @Override
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        skipDuplicateTestCoverage("testDropAmbiguousRowFieldCaseSensitivity");
    }

    @Test
    @Override
    public void testDropNonEmptySchemaWithMaterializedView()
    {
        skipDuplicateTestCoverage("testDropNonEmptySchemaWithMaterializedView");
    }

    @Test
    @Override
    public void testDropNonEmptySchemaWithView()
    {
        skipDuplicateTestCoverage("testDropNonEmptySchemaWithView");
    }

    @Test
    @Override
    public void testDropRowField()
    {
        skipDuplicateTestCoverage("testDropRowField");
    }

    @Test
    @Override
    public void testDropRowFieldCaseSensitivity()
    {
        skipDuplicateTestCoverage("testDropRowFieldCaseSensitivity");
    }

    @Test
    @Override
    public void testDropRowFieldWhenDuplicates()
    {
        skipDuplicateTestCoverage("testDropRowFieldWhenDuplicates");
    }

    @Test
    @Override
    public void testDropSchemaCascade()
    {
        skipDuplicateTestCoverage("testDropSchemaCascade");
    }

    @Test
    @Override
    public void testDropTable()
    {
        skipDuplicateTestCoverage("testDropTable");
    }

    @Test
    @Override
    public void testDropTableIfExists()
    {
        skipDuplicateTestCoverage("testDropTableIfExists");
    }

    @Test
    @Override
    public void testExactPredicate()
    {
        skipDuplicateTestCoverage("testExactPredicate");
    }

    @Test
    @Override
    public void testExplainAnalyze()
    {
        skipDuplicateTestCoverage("testExplainAnalyze");
    }

    @Test
    @Override
    public void testExplainAnalyzeVerbose()
    {
        skipDuplicateTestCoverage("testExplainAnalyzeVerbose");
    }

    @Test
    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        skipDuplicateTestCoverage("testExplainAnalyzeWithDeleteWithSubquery");
    }

    @Test
    @Override
    public void testFederatedMaterializedView()
    {
        skipDuplicateTestCoverage("testFederatedMaterializedView");
    }

    @Test
    @Override
    public void testFederatedMaterializedViewWithGracePeriod()
    {
        skipDuplicateTestCoverage("testFederatedMaterializedViewWithGracePeriod");
    }

    @Test
    @Override
    public void testInListPredicate()
    {
        skipDuplicateTestCoverage("testInListPredicate");
    }

    @Test
    @Override
    public void testInsert()
    {
        skipDuplicateTestCoverage("testInsert");
    }

    @Test
    @Override
    public void testInsertArray()
    {
        skipDuplicateTestCoverage("testInsertArray");
    }

    @Test
    @Override
    public void testInsertForDefaultColumn()
    {
        skipDuplicateTestCoverage("testInsertForDefaultColumn");
    }

    @Test
    @Override
    public void testInsertHighestUnicodeCharacter()
    {
        skipDuplicateTestCoverage("testInsertHighestUnicodeCharacter");
    }

    @Test
    @Override
    public void testInsertInTransaction()
    {
        skipDuplicateTestCoverage("testInsertInTransaction");
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        skipDuplicateTestCoverage("testInsertIntoNotNullColumn");
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        skipDuplicateTestCoverage("testInsertNegativeDate");
    }

    @Test
    @Override
    public void testInsertRowConcurrently()
    {
        skipDuplicateTestCoverage("testInsertRowConcurrently");
    }

    @Test
    @Override
    public void testInsertSameValues()
    {
        skipDuplicateTestCoverage("testInsertSameValues");
    }

    @Test
    @Override
    public void testInsertUnicode()
    {
        skipDuplicateTestCoverage("testInsertUnicode");
    }

    @Test
    @Override
    public void testIsNullPredicate()
    {
        skipDuplicateTestCoverage("testIsNullPredicate");
    }

    @Test
    @Override
    public void testJoin()
    {
        skipDuplicateTestCoverage("testJoin");
    }

    @Test
    @Override
    public void testJoinWithEmptySides(OptimizerConfig.JoinDistributionType arg0)
    {
        skipDuplicateTestCoverage("testJoinWithEmptySides", OptimizerConfig.JoinDistributionType.class);
    }

    @Test
    @Override
    public void testLikePredicate()
    {
        skipDuplicateTestCoverage("testLikePredicate");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        skipDuplicateTestCoverage("testMaterializedView");
    }

    @Test
    @Override
    public void testMaterializedViewAllTypes()
    {
        skipDuplicateTestCoverage("testMaterializedViewAllTypes");
    }

    @Test
    @Override
    public void testMaterializedViewBaseTableGone(boolean arg0)
    {
        skipDuplicateTestCoverage("testMaterializedViewBaseTableGone", boolean.class);
    }

    @Test
    @Override
    public void testMaterializedViewColumnName(String arg0)
    {
        skipDuplicateTestCoverage("testMaterializedViewColumnName", String.class);
    }

    @Test
    @Override
    public void testMaterializedViewGracePeriod()
    {
        skipDuplicateTestCoverage("testMaterializedViewGracePeriod");
    }

    @Test
    @Override
    public void testMergeAllColumnsReversed()
    {
        skipDuplicateTestCoverage("testMergeAllColumnsReversed");
    }

    @Test
    @Override
    public void testMergeAllColumnsUpdated()
    {
        skipDuplicateTestCoverage("testMergeAllColumnsUpdated");
    }

    @Test
    @Override
    public void testMergeAllInserts()
    {
        skipDuplicateTestCoverage("testMergeAllInserts");
    }

    @Test
    @Override
    public void testMergeAllMatchesDeleted()
    {
        skipDuplicateTestCoverage("testMergeAllMatchesDeleted");
    }

    @Test
    @Override
    public void testMergeCasts()
    {
        skipDuplicateTestCoverage("testMergeCasts");
    }

    @Test
    @Override
    public void testMergeDeleteWithCTAS()
    {
        skipDuplicateTestCoverage("testMergeDeleteWithCTAS");
    }

    @Test
    @Override
    public void testMergeFalseJoinCondition()
    {
        skipDuplicateTestCoverage("testMergeFalseJoinCondition");
    }

    @Test
    @Override
    public void testMergeFruits()
    {
        skipDuplicateTestCoverage("testMergeFruits");
    }

    @Test
    @Override
    public void testMergeLarge()
    {
        skipDuplicateTestCoverage("testMergeLarge");
    }

    @Test
    @Override
    public void testMergeMultipleOperations()
    {
        skipDuplicateTestCoverage("testMergeMultipleOperations");
    }

    @Test
    @Override
    public void testMergeMultipleRowsMatchFails()
    {
        skipDuplicateTestCoverage("testMergeMultipleRowsMatchFails");
    }

    @Test
    @Override
    public void testMergeNonNullableColumns()
    {
        skipDuplicateTestCoverage("testMergeNonNullableColumns");
    }

    @Test
    @Override
    public void testMergeQueryWithStrangeCapitalization()
    {
        skipDuplicateTestCoverage("testMergeQueryWithStrangeCapitalization");
    }

    @Test
    @Override
    public void testMergeSimpleQuery()
    {
        skipDuplicateTestCoverage("testMergeSimpleQuery");
    }

    @Test
    @Override
    public void testMergeSimpleSelect()
    {
        skipDuplicateTestCoverage("testMergeSimpleSelect");
    }

    @Test
    @Override
    public void testMergeSubqueries()
    {
        skipDuplicateTestCoverage("testMergeSubqueries");
    }

    @Test
    @Override
    public void testMergeWithSimplifiedUnpredictablePredicates()
    {
        skipDuplicateTestCoverage("testMergeWithSimplifiedUnpredictablePredicates");
    }

    @Test
    @Override
    public void testMergeWithUnpredictablePredicates()
    {
        skipDuplicateTestCoverage("testMergeWithUnpredictablePredicates");
    }

    @Test
    @Override
    public void testMergeWithoutTablesAliases()
    {
        skipDuplicateTestCoverage("testMergeWithoutTablesAliases");
    }

    @Test
    @Override
    public void testMultipleRangesPredicate()
    {
        skipDuplicateTestCoverage("testMultipleRangesPredicate");
    }

    @Test
    @Override
    public void testNoDataSystemTable()
    {
        skipDuplicateTestCoverage("testNoDataSystemTable");
    }

    @Test
    @Override
    public void testPotentialDuplicateDereferencePushdown()
    {
        skipDuplicateTestCoverage("testPotentialDuplicateDereferencePushdown");
    }

    @Test
    @Override
    public void testPredicateOnRowTypeField()
    {
        skipDuplicateTestCoverage("testPredicateOnRowTypeField");
    }

    @Test
    @Override
    public void testPredicateReflectedInExplain()
    {
        skipDuplicateTestCoverage("testPredicateReflectedInExplain");
    }

    @Test
    @Override
    public void testProjectionPushdown()
    {
        skipDuplicateTestCoverage("testProjectionPushdown");
    }

    @Test
    @Override
    public void testProjectionPushdownMultipleRows()
    {
        skipDuplicateTestCoverage("testProjectionPushdownMultipleRows");
    }

    @Test
    @Override
    public void testProjectionPushdownPhysicalInputSize()
    {
        skipDuplicateTestCoverage("testProjectionPushdownPhysicalInputSize");
    }

    @Test
    @Override
    public void testProjectionPushdownReadsLessData()
    {
        skipDuplicateTestCoverage("testProjectionPushdownReadsLessData");
    }

    @Test
    @Override
    public void testProjectionPushdownWithHighlyNestedData()
    {
        skipDuplicateTestCoverage("testProjectionPushdownWithHighlyNestedData");
    }

    @Test
    @Override
    public void testProjectionWithCaseSensitiveField()
    {
        skipDuplicateTestCoverage("testProjectionWithCaseSensitiveField");
    }

    @Test
    @Override
    public void testQueryLoggingCount()
    {
        skipDuplicateTestCoverage("testQueryLoggingCount");
    }

    @Test
    @Override
    public void testRangePredicate()
    {
        skipDuplicateTestCoverage("testRangePredicate");
    }

    @Test
    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        skipDuplicateTestCoverage("testReadMetadataWithRelationsConcurrentModifications");
    }

    @Test
    @Override
    public void testRenameMaterializedView()
    {
        skipDuplicateTestCoverage("testRenameMaterializedView");
    }

    @Test
    @Override
    public void testRenameRowField()
    {
        skipDuplicateTestCoverage("testRenameRowField");
    }

    @Test
    @Override
    public void testRenameRowFieldCaseSensitivity()
    {
        skipDuplicateTestCoverage("testRenameRowFieldCaseSensitivity");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        skipDuplicateTestCoverage("testRenameSchema");
    }

    @Test
    @Override
    public void testRenameSchemaToLongName()
    {
        skipDuplicateTestCoverage("testRenameSchemaToLongName");
    }

    @Test
    @Override
    public void testRenameTable()
    {
        skipDuplicateTestCoverage("testRenameTable");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchema()
    {
        skipDuplicateTestCoverage("testRenameTableAcrossSchema");
    }

    @Test
    @Override
    public void testRenameTableToLongTableName()
    {
        skipDuplicateTestCoverage("testRenameTableToLongTableName");
    }

    @Test
    @Override
    public void testRenameTableToUnqualifiedPreservesSchema()
    {
        skipDuplicateTestCoverage("testRenameTableToUnqualifiedPreservesSchema");
    }

    @Test
    @Override
    public void testRollback()
    {
        skipDuplicateTestCoverage("testRollback");
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        skipDuplicateTestCoverage("testRowLevelDelete");
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        skipDuplicateTestCoverage("testRowLevelUpdate");
    }

    @Test
    @Override
    public void testSelectAfterInsertInTransaction()
    {
        skipDuplicateTestCoverage("testSelectAfterInsertInTransaction");
    }

    @Test
    @Override
    public void testSelectAll()
    {
        skipDuplicateTestCoverage("testSelectAll");
    }

    @Test
    @Override
    public void testSelectInTransaction()
    {
        skipDuplicateTestCoverage("testSelectInTransaction");
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        skipDuplicateTestCoverage("testSelectInformationSchemaColumns");
    }

    @Test
    @Override
    public void testSelectInformationSchemaTables()
    {
        skipDuplicateTestCoverage("testSelectInformationSchemaTables");
    }

    @Test
    @Override
    public void testSelectVersionOfNonExistentTable()
    {
        skipDuplicateTestCoverage("testSelectVersionOfNonExistentTable");
    }

    @Test
    @Override
    public void testSetColumnIncompatibleType()
    {
        skipDuplicateTestCoverage("testSetColumnIncompatibleType");
    }

    @Test
    @Override
    public void testSetColumnOutOfRangeType()
    {
        skipDuplicateTestCoverage("testSetColumnOutOfRangeType");
    }

    @Test
    @Override
    public void testSetColumnType()
    {
        skipDuplicateTestCoverage("testSetColumnType");
    }

    @Test
    @Override
    public void testSetColumnTypeWithComment()
    {
        skipDuplicateTestCoverage("testSetColumnTypeWithComment");
    }

    @Test
    @Override
    public void testSetColumnTypeWithDefaultColumn()
    {
        skipDuplicateTestCoverage("testSetColumnTypeWithDefaultColumn");
    }

    @Test
    @Override
    public void testSetColumnTypeWithNotNull()
    {
        skipDuplicateTestCoverage("testSetColumnTypeWithNotNull");
    }

    @Test
    @Override
    public void testSetColumnTypes(BaseConnectorTest.SetColumnTypeSetup arg0)
    {
        skipDuplicateTestCoverage("testSetColumnTypes", BaseConnectorTest.SetColumnTypeSetup.class);
    }

    @Test
    @Override
    public void testSetFieldIncompatibleType()
    {
        skipDuplicateTestCoverage("testSetFieldIncompatibleType");
    }

    @Test
    @Override
    public void testSetFieldOutOfRangeType()
    {
        skipDuplicateTestCoverage("testSetFieldOutOfRangeType");
    }

    @Test
    @Override
    public void testSetFieldType()
    {
        skipDuplicateTestCoverage("testSetFieldType");
    }

    @Test
    @Override
    public void testSetFieldTypeCaseSensitivity()
    {
        skipDuplicateTestCoverage("testSetFieldTypeCaseSensitivity");
    }

    @Test
    @Override
    public void testSetFieldTypeWithComment()
    {
        skipDuplicateTestCoverage("testSetFieldTypeWithComment");
    }

    @Test
    @Override
    public void testSetFieldTypeWithNotNull()
    {
        skipDuplicateTestCoverage("testSetFieldTypeWithNotNull");
    }

    @Test
    @Override
    public void testSetFieldTypes(BaseConnectorTest.SetColumnTypeSetup arg0)
    {
        skipDuplicateTestCoverage("testSetFieldTypes", BaseConnectorTest.SetColumnTypeSetup.class);
    }

    @Test
    @Override
    public void testShowCreateInformationSchema()
    {
        skipDuplicateTestCoverage("testShowCreateInformationSchema");
    }

    @Test
    @Override
    public void testShowCreateInformationSchemaTable()
    {
        skipDuplicateTestCoverage("testShowCreateInformationSchemaTable");
    }

    @Test
    @Override
    public void testShowCreateView()
    {
        skipDuplicateTestCoverage("testShowCreateView");
    }

    @Test
    @Override
    public void testShowInformationSchemaTables()
    {
        skipDuplicateTestCoverage("testShowInformationSchemaTables");
    }

    @Test
    @Override
    public void testShowSchemasFromOther()
    {
        skipDuplicateTestCoverage("testShowSchemasFromOther");
    }

    @Test
    @Override
    public void testSortItemsReflectedInExplain()
    {
        skipDuplicateTestCoverage("testSortItemsReflectedInExplain");
    }

    @Test
    @Override
    public void testSymbolAliasing()
    {
        skipDuplicateTestCoverage("testSymbolAliasing");
    }

    @Test
    @Override
    public void testTableSampleSystem()
    {
        skipDuplicateTestCoverage("testTableSampleSystem");
    }

    @Test
    @Override
    public void testTableSampleWithFiltering()
    {
        skipDuplicateTestCoverage("testTableSampleWithFiltering");
    }

    @Test
    @Override
    public void testTruncateTable()
    {
        skipDuplicateTestCoverage("testTruncateTable");
    }

    @Test
    @Override
    public void testTrySelectTableVersion()
    {
        skipDuplicateTestCoverage("testTrySelectTableVersion");
    }

    @Test
    @Override
    public void testUpdate()
    {
        skipDuplicateTestCoverage("testUpdate");
    }

    @Test
    @Override
    public void testUpdateAllValues()
    {
        skipDuplicateTestCoverage("testUpdateAllValues");
    }

    @Test
    @Override
    public void testUpdateNotNullColumn()
    {
        skipDuplicateTestCoverage("testUpdateNotNullColumn");
    }

    @Test
    @Override
    public void testUpdateRowConcurrently()
    {
        skipDuplicateTestCoverage("testUpdateRowConcurrently");
    }

    @Test
    @Override
    public void testUpdateRowType()
    {
        skipDuplicateTestCoverage("testUpdateRowType");
    }

    @Test
    @Override
    public void testUpdateWithPredicates()
    {
        skipDuplicateTestCoverage("testUpdateWithPredicates");
    }

    @Test
    @Override
    public void testVarcharCastToDateInPredicate()
    {
        skipDuplicateTestCoverage("testVarcharCastToDateInPredicate");
    }

    @Test
    @Override
    public void testVarcharCharComparison()
    {
        skipDuplicateTestCoverage("testVarcharCharComparison");
    }

    @Test
    @Override
    public void testView()
    {
        skipDuplicateTestCoverage("testView");
    }

    @Test
    @Override
    public void testViewAndMaterializedViewTogether()
    {
        skipDuplicateTestCoverage("testViewAndMaterializedViewTogether");
    }

    @Test
    @Override
    public void testViewCaseSensitivity()
    {
        skipDuplicateTestCoverage("testViewCaseSensitivity");
    }

    @Test
    @Override
    public void testViewMetadata(String arg0, String arg1)
    {
        skipDuplicateTestCoverage("testViewMetadata", String.class, String.class);
    }

    @Test
    @Override
    public void testWriteNotAllowedInTransaction()
    {
        skipDuplicateTestCoverage("testWriteNotAllowedInTransaction");
    }

    @Test
    @Override
    public void testWrittenDataSize()
    {
        skipDuplicateTestCoverage("testWrittenDataSize");
    }

    @Test
    @Override
    public void testWrittenStats()
    {
        skipDuplicateTestCoverage("testWrittenStats");
    }

    @Test
    @Override
    public void verifySupportsDeleteDeclaration()
    {
        skipDuplicateTestCoverage("verifySupportsDeleteDeclaration");
    }

    @Test
    @Override
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        skipDuplicateTestCoverage("verifySupportsRowLevelDeleteDeclaration");
    }

    @Test
    @Override
    public void verifySupportsRowLevelUpdateDeclaration()
    {
        skipDuplicateTestCoverage("verifySupportsRowLevelUpdateDeclaration");
    }

    @Test
    @Override
    public void verifySupportsUpdateDeclaration()
    {
        skipDuplicateTestCoverage("verifySupportsUpdateDeclaration");
    }
}
