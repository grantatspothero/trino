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
import io.trino.Session;
import io.trino.metadata.TableMetadata;
import io.trino.plugin.hive.BaseHiveConnectorTest;
import io.trino.plugin.hive.HiveConnector;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.objectstore.ConnectorFeaturesTestHelper.TestFramework;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveQueryRunner.copyTpchTablesBucketed;
import static io.trino.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.initializeTpchTables;
import static io.trino.plugin.tpch.ColumnNaming.SIMPLIFIED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests ObjectStore connector with Hive backend, exercising all
 * hive-specific tests inherited from {@link BaseHiveConnectorTest}.
 *
 * @see TestObjectStoreHiveConnectorTest
 * @see TestObjectStoreIcebergFeaturesConnectorTest
 * @see TestObjectStoreDeltaFeaturesConnectorTest
 */
public class TestObjectStoreHiveFeaturesConnectorTest
        extends BaseHiveConnectorTest
{
    private static final ConnectorFeaturesTestHelper HELPER = new ConnectorFeaturesTestHelper(TestObjectStoreHiveFeaturesConnectorTest.class, TestObjectStoreHiveConnectorTest.class);

    private TestFramework testFramework;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String catalog = HiveQueryRunner.HIVE_CATALOG;
        String schema = "tpch";
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(catalog)
                                .setSchema(schema)
                                .build())
                // This is needed for e2e scale writers test otherwise 50% threshold of
                // bufferSize won't get exceeded for scaling to happen (synced from BaseHiveConnectorTest)
                .addExtraProperty("task.max-local-exchange-buffer-size", "32MB")
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", Map.of());

            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());

            String metastoreDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toString();
            Map<String, String> objectStoreProperties = ImmutableMap.<String, String>builder()
                    // Hive
                    .put("HIVE__hive.metastore", "file")
                    .put("HIVE__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("HIVE__galaxy.location-security.enabled", "false")
                    .put("HIVE__galaxy.account-url", "https://localhost:1234")
                    .put("HIVE__galaxy.catalog-id", "c-1234567890")
                    // Hive setting synced from BaseHiveConnectorTest
                    .put("HIVE__hive.allow-register-partition-procedure", "true")
                    // Reduce writer sort buffer size to ensure SortingFileWriter gets used
                    .put("HIVE__hive.writer-sort-buffer-size", "1MB")
                    // Make weighted split scheduling more conservative to avoid OOMs in test
                    .put("HIVE__hive.minimum-assigned-split-weight", "0.5")
                    .put("HIVE__hive.partition-projection-enabled", "true")
                    // Hive setting synced from HiveQueryRunner
                    .put("HIVE__hive.max-partitions-per-scan", "1000")
                    .put("HIVE__hive.max-partitions-for-eager-load", "1000")
                    // Iceberg
                    .put("ICEBERG__iceberg.catalog.type", "TESTING_FILE_METASTORE")
                    .put("ICEBERG__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("ICEBERG__galaxy.location-security.enabled", "false")
                    .put("ICEBERG__galaxy.account-url", "https://localhost:1234")
                    .put("ICEBERG__galaxy.catalog-id", "c-1234567890")
                    // Delta
                    .put("DELTA__hive.metastore", "file")
                    .put("DELTA__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("DELTA__galaxy.location-security.enabled", "false")
                    .put("DELTA__galaxy.account-url", "https://localhost:1234")
                    .put("DELTA__galaxy.catalog-id", "c-1234567890")
                    // Hudi
                    .put("HUDI__hive.metastore", "file")
                    .put("HUDI__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("HUDI__galaxy.location-security.enabled", "false")
                    .put("HUDI__galaxy.account-url", "https://localhost:1234")
                    .put("HUDI__galaxy.catalog-id", "c-1234567890")
                    // ObjectStore
                    .put("OBJECTSTORE__object-store.table-type", TableType.HIVE.name())
                    .put("OBJECTSTORE__galaxy.location-security.enabled", "false")
                    .put("OBJECTSTORE__galaxy.account-url", "https://localhost:1234")
                    .put("OBJECTSTORE__galaxy.catalog-id", "c-1234567890")
                    .buildOrThrow();
            queryRunner.createCatalog(catalog, "galaxy_objectstore", objectStoreProperties);

            // Hive setting synced from HiveQueryRunner
            Map<String, String> objectStoreBucketedProperties = new HashMap<>(objectStoreProperties);
            objectStoreBucketedProperties.put("HIVE__hive.max-initial-split-size", "10kB"); // so that each bucket has multiple splits
            objectStoreBucketedProperties.put("HIVE__hive.max-split-size", "10kB"); // so that each bucket has multiple splits
            objectStoreBucketedProperties.put("HIVE__hive.storage-format", "TEXTFILE"); // so that there's no minimum split size for the file
            objectStoreBucketedProperties.put("HIVE__hive.compression-codec", "NONE"); // so that the file is splittable
            String bucketedCatalog = HiveQueryRunner.HIVE_BUCKETED_CATALOG;
            queryRunner.createCatalog(bucketedCatalog, "galaxy_objectstore", objectStoreBucketedProperties);

            queryRunner.execute("CREATE SCHEMA %s.%s".formatted(catalog, schema));
            initializeTpchTables(queryRunner, REQUIRED_TPCH_TABLES);

            String bucketedSchema = HiveQueryRunner.TPCH_BUCKETED_SCHEMA;
            queryRunner.execute("CREATE SCHEMA %s.%s".formatted(bucketedCatalog, bucketedSchema));
            copyTpchTablesBucketed(
                    queryRunner,
                    "tpch",
                    "tiny",
                    Session.builder(queryRunner.getDefaultSession())
                            .setCatalog(bucketedCatalog)
                            .setSchema(bucketedSchema)
                            .build(),
                    REQUIRED_TPCH_TABLES,
                    SIMPLIFIED);

            // extra catalog with NANOSECOND timestamp precision
            Map<String, String> objectStoreHiveNanosProperties = new HashMap<>(objectStoreProperties);
            objectStoreHiveNanosProperties.put("HIVE__hive.timestamp-precision", "NANOSECONDS");
            queryRunner.createCatalog(
                    "hive_timestamp_nanos",
                    "galaxy_objectstore",
                    objectStoreHiveNanosProperties);

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

    @Override
    protected HiveConnector getHiveConnector(String catalog)
    {
        ObjectStoreConnector objectStoreConnector = (ObjectStoreConnector) getDistributedQueryRunner().getCoordinator().getConnector(catalog);
        return (HiveConnector) objectStoreConnector.getInjector().getInstance(DelegateConnectors.class).hiveConnector();
    }

    @Override
    protected TableMetadata getTableMetadata(String catalog, String schema, String tableName)
    {
        TableMetadata tableMetadata = super.getTableMetadata(catalog, schema, tableName);

        // wrap it back
        return new TableMetadata(
                tableMetadata.getCatalogName(),
                new ConnectorTableMetadata(
                        tableMetadata.getMetadata().getTable(),
                        tableMetadata.getMetadata().getColumns(),
                        tableMetadata.getMetadata().getProperties().entrySet().stream()
                                .map(entry -> entry(entry.getKey(), switch (entry.getKey()) {
                                    case STORAGE_FORMAT_PROPERTY -> HiveStorageFormat.valueOf((String) entry.getValue());
                                    default -> entry.getValue();
                                }))
                                .collect(toImmutableMap(Entry::getKey, Entry::getValue)),
                        tableMetadata.getMetadata().getComment(),
                        tableMetadata.getMetadata().getCheckConstraints()));
    }

    @Override
    public void testCreateSchemaWithAuthorizationForUser()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testCreateSchemaWithAuthorizationForRole()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testSchemaAuthorization()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testSchemaAuthorizationForUser()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testSchemaAuthorizationForRole()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testTableAuthorization()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testTableAuthorizationForRole()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testViewAuthorization()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testViewAuthorizationForRole()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testViewAuthorizationSecurityDefiner()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testViewAuthorizationSecurityInvoker()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testShowTablePrivileges()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testShowColumnMetadata()
    {
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testCurrentUserInView()
    {
        // The test merit is not Galaxy specific, but the test structure is.
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testShowCreateSchema()
    {
        // The test merit is not Galaxy specific, but the test structure is.
        skipAuthorizationRelatedTest();
    }

    @Override
    public void testExtraProperties()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testExtraProperties)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Override
    public void testExtraPropertiesWithCtas()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testExtraPropertiesWithCtas)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Override
    public void testShowCreateWithExtraProperties()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testShowCreateWithExtraProperties)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Override
    public void testDuplicateExtraProperties()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testDuplicateExtraProperties)
                .hasMessageContaining("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Override
    public void testOverwriteExistingPropertyWithExtraProperties()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testOverwriteExistingPropertyWithExtraProperties)
                .hasMessageContaining("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Override
    public void testNullExtraProperty()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testNullExtraProperty)
                .hasMessageContaining("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Override
    public void testCollidingMixedCaseProperty()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testCollidingMixedCaseProperty)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Override
    public void testCreateAndInsert()
    {
        assertThatThrownBy(super::testCreateAndInsert)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog only supports writes using autocommit: hive");
    }

    @Override
    public void testDeleteAndInsert()
    {
        assertThatThrownBy(super::testDeleteAndInsert)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog only supports writes using autocommit: hive");
    }

    @Override
    public void testInsertIntoPartitionedBucketedTableFromBucketedTable()
    {
        assertThatThrownBy(super::testInsertIntoPartitionedBucketedTableFromBucketedTable)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog only supports writes using autocommit: hive");
    }

    @Override
    public void testMismatchedBucketing()
    {
        assertThatThrownBy(super::testMismatchedBucketing)
                .hasStackTraceContaining("Not in a transaction");
    }

    @Override
    public void testOptimize()
    {
        assertThatThrownBy(super::testOptimize)
                .hasMessageContaining("Executing OPTIMIZE on Hive tables is not supported");
    }

    @Override
    public void testOptimizeWithPartitioning()
    {
        assertThatThrownBy(super::testOptimizeWithPartitioning)
                .hasMessageContaining("Executing OPTIMIZE on Hive tables is not supported");
    }

    @Override
    public void testOptimizeWithBucketing()
    {
        assertThatThrownBy(super::testOptimizeWithBucketing)
                .hasMessageContaining("Executing OPTIMIZE on Hive tables is not supported");
    }

    @Override
    public void testOptimizeWithWriterScaling()
    {
        assertThatThrownBy(super::testOptimizeWithWriterScaling)
                .hasMessageContaining("Executing OPTIMIZE on Hive tables is not supported");
    }

    @BeforeMethod(alwaysRun = true)
    public void preventDuplicatedTestCoverage(Method testMethod)
    {
        HELPER.preventDuplicatedTestCoverage(testMethod);
    }

    private void skipAuthorizationRelatedTest()
    {
        throw new SkipException("Test is disabled. Hive authorization & roles work differently in Galaxy");
    }

    @Override
    public void ensureDistributedQueryRunner()
    {
        // duplicate test, but still desired to run
        super.ensureDistributedQueryRunner();
    }

    @Override
    public void ensureTestNamingConvention()
    {
        // duplicate test, but still desired to run
        super.ensureTestNamingConvention();
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

    @Override
    public void testAddAndDropColumnName(String arg0)
    {
        skipDuplicateTestCoverage("testAddAndDropColumnName", String.class);
    }

    @Override
    public void testAddColumn()
    {
        skipDuplicateTestCoverage("testAddColumn");
    }

    @Override
    public void testAddColumnConcurrently()
    {
        skipDuplicateTestCoverage("testAddColumnConcurrently");
    }

    @Override
    public void testAddColumnWithComment()
    {
        skipDuplicateTestCoverage("testAddColumnWithComment");
    }

    @Override
    public void testAddColumnWithCommentSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testAddColumnWithCommentSpecialCharacter", String.class);
    }

    @Override
    public void testAddNotNullColumn()
    {
        skipDuplicateTestCoverage("testAddNotNullColumn");
    }

    @Override
    public void testAddNotNullColumnToEmptyTable()
    {
        skipDuplicateTestCoverage("testAddNotNullColumnToEmptyTable");
    }

    @Override
    public void testAddRowField()
    {
        skipDuplicateTestCoverage("testAddRowField");
    }

    @Override
    public void testAggregation()
    {
        skipDuplicateTestCoverage("testAggregation");
    }

    @Override
    public void testAlterTableAddLongColumnName()
    {
        skipDuplicateTestCoverage("testAlterTableAddLongColumnName");
    }

    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        skipDuplicateTestCoverage("testAlterTableRenameColumnToLongName");
    }

    @Override
    public void testCaseSensitiveDataMapping(BaseConnectorTest.DataMappingTestSetup arg0)
    {
        skipDuplicateTestCoverage("testCaseSensitiveDataMapping", BaseConnectorTest.DataMappingTestSetup.class);
    }

    @Override
    public void testCharVarcharComparison()
    {
        skipDuplicateTestCoverage("testCharVarcharComparison");
    }

    @Override
    public void testColumnCommentMaterializedView()
    {
        skipDuplicateTestCoverage("testColumnCommentMaterializedView");
    }

    @Override
    public void testColumnName(String arg0)
    {
        skipDuplicateTestCoverage("testColumnName", String.class);
    }

    @Override
    public void testColumnsInReverseOrder()
    {
        skipDuplicateTestCoverage("testColumnsInReverseOrder");
    }

    @Override
    public void testCommentColumn()
    {
        skipDuplicateTestCoverage("testCommentColumn");
    }

    @Override
    public void testCommentColumnName(String arg0)
    {
        skipDuplicateTestCoverage("testCommentColumnName", String.class);
    }

    @Override
    public void testCommentColumnSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testCommentColumnSpecialCharacter", String.class);
    }

    @Override
    public void testCommentTable()
    {
        skipDuplicateTestCoverage("testCommentTable");
    }

    @Override
    public void testCommentTableSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testCommentTableSpecialCharacter", String.class);
    }

    @Override
    public void testCommentView()
    {
        skipDuplicateTestCoverage("testCommentView");
    }

    @Override
    public void testCommentViewColumn()
    {
        skipDuplicateTestCoverage("testCommentViewColumn");
    }

    @Override
    public void testCompatibleTypeChangeForView()
    {
        skipDuplicateTestCoverage("testCompatibleTypeChangeForView");
    }

    @Override
    public void testCompatibleTypeChangeForView2()
    {
        skipDuplicateTestCoverage("testCompatibleTypeChangeForView2");
    }

    @Override
    public void testConcurrentScans()
    {
        skipDuplicateTestCoverage("testConcurrentScans");
    }

    @Override
    public void testCreateSchema()
    {
        skipDuplicateTestCoverage("testCreateSchema");
    }

    @Override
    public void testCreateSchemaWithLongName()
    {
        skipDuplicateTestCoverage("testCreateSchemaWithLongName");
    }

    @Override
    public void testCreateTable()
    {
        skipDuplicateTestCoverage("testCreateTable");
    }

    @Override
    public void testCreateTableAsSelect()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelect");
    }

    @Override
    public void testCreateTableAsSelectNegativeDate()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectNegativeDate");
    }

    @Override
    public void testCreateTableAsSelectSchemaNotFound()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectSchemaNotFound");
    }

    @Override
    public void testCreateTableAsSelectWithTableComment()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectWithTableComment");
    }

    @Override
    public void testCreateTableAsSelectWithTableCommentSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectWithTableCommentSpecialCharacter", String.class);
    }

    @Override
    public void testCreateTableAsSelectWithUnicode()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectWithUnicode");
    }

    @Override
    public void testCreateTableSchemaNotFound()
    {
        skipDuplicateTestCoverage("testCreateTableSchemaNotFound");
    }

    @Override
    public void testCreateTableWithColumnComment()
    {
        skipDuplicateTestCoverage("testCreateTableWithColumnComment");
    }

    @Override
    public void testCreateTableWithColumnCommentSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testCreateTableWithColumnCommentSpecialCharacter", String.class);
    }

    @Override
    public void testCreateTableWithLongColumnName()
    {
        skipDuplicateTestCoverage("testCreateTableWithLongColumnName");
    }

    @Override
    public void testCreateTableWithLongTableName()
    {
        skipDuplicateTestCoverage("testCreateTableWithLongTableName");
    }

    @Override
    public void testCreateTableWithTableComment()
    {
        skipDuplicateTestCoverage("testCreateTableWithTableComment");
    }

    @Override
    public void testCreateTableWithTableCommentSpecialCharacter(String arg0)
    {
        skipDuplicateTestCoverage("testCreateTableWithTableCommentSpecialCharacter", String.class);
    }

    @Override
    public void testCreateViewSchemaNotFound()
    {
        skipDuplicateTestCoverage("testCreateViewSchemaNotFound");
    }

    @Override
    public void testDataMappingSmokeTest(BaseConnectorTest.DataMappingTestSetup arg0)
    {
        skipDuplicateTestCoverage("testDataMappingSmokeTest", BaseConnectorTest.DataMappingTestSetup.class);
    }

    @Override
    public void testDateYearOfEraPredicate()
    {
        skipDuplicateTestCoverage("testDateYearOfEraPredicate");
    }

    @Override
    public void testDeleteAllDataFromTable()
    {
        skipDuplicateTestCoverage("testDeleteAllDataFromTable");
    }

    @Override
    public void testDescribeTable()
    {
        skipDuplicateTestCoverage("testDescribeTable");
    }

    @Override
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        skipDuplicateTestCoverage("testDropAmbiguousRowFieldCaseSensitivity");
    }

    @Override
    public void testDropNonEmptySchemaWithMaterializedView()
    {
        skipDuplicateTestCoverage("testDropNonEmptySchemaWithMaterializedView");
    }

    @Override
    public void testDropNonEmptySchemaWithTable()
    {
        skipDuplicateTestCoverage("testDropNonEmptySchemaWithTable");
    }

    @Override
    public void testDropNonEmptySchemaWithView()
    {
        skipDuplicateTestCoverage("testDropNonEmptySchemaWithView");
    }

    @Override
    public void testDropRowField()
    {
        skipDuplicateTestCoverage("testDropRowField");
    }

    @Override
    public void testDropRowFieldCaseSensitivity()
    {
        skipDuplicateTestCoverage("testDropRowFieldCaseSensitivity");
    }

    @Override
    public void testDropRowFieldWhenDuplicates()
    {
        skipDuplicateTestCoverage("testDropRowFieldWhenDuplicates");
    }

    @Override
    public void testDropSchemaCascade()
    {
        skipDuplicateTestCoverage("testDropSchemaCascade");
    }

    @Override
    public void testDropTable()
    {
        skipDuplicateTestCoverage("testDropTable");
    }

    @Override
    public void testDropTableIfExists()
    {
        skipDuplicateTestCoverage("testDropTableIfExists");
    }

    @Override
    public void testExactPredicate()
    {
        skipDuplicateTestCoverage("testExactPredicate");
    }

    @Override
    public void testExplainAnalyze()
    {
        skipDuplicateTestCoverage("testExplainAnalyze");
    }

    @Override
    public void testExplainAnalyzeVerbose()
    {
        skipDuplicateTestCoverage("testExplainAnalyzeVerbose");
    }

    @Override
    public void testFederatedMaterializedView()
    {
        skipDuplicateTestCoverage("testFederatedMaterializedView");
    }

    @Override
    public void testFederatedMaterializedViewWithGracePeriod()
    {
        skipDuplicateTestCoverage("testFederatedMaterializedViewWithGracePeriod");
    }

    @Override
    public void testInListPredicate()
    {
        skipDuplicateTestCoverage("testInListPredicate");
    }

    @Override
    public void testInsertArray()
    {
        skipDuplicateTestCoverage("testInsertArray");
    }

    @Override
    public void testInsertForDefaultColumn()
    {
        skipDuplicateTestCoverage("testInsertForDefaultColumn");
    }

    @Override
    public void testInsertInTransaction()
    {
        skipDuplicateTestCoverage("testInsertInTransaction");
    }

    @Override
    public void testInsertIntoNotNullColumn()
    {
        skipDuplicateTestCoverage("testInsertIntoNotNullColumn");
    }

    @Override
    public void testInsertNegativeDate()
    {
        skipDuplicateTestCoverage("testInsertNegativeDate");
    }

    @Override
    public void testInsertRowConcurrently()
    {
        skipDuplicateTestCoverage("testInsertRowConcurrently");
    }

    @Override
    public void testInsertSameValues()
    {
        skipDuplicateTestCoverage("testInsertSameValues");
    }

    @Override
    public void testIsNullPredicate()
    {
        skipDuplicateTestCoverage("testIsNullPredicate");
    }

    @Override
    public void testJoin()
    {
        skipDuplicateTestCoverage("testJoin");
    }

    @Override
    public void testJoinWithEmptySides(OptimizerConfig.JoinDistributionType arg0)
    {
        skipDuplicateTestCoverage("testJoinWithEmptySides", OptimizerConfig.JoinDistributionType.class);
    }

    @Override
    public void testLikePredicate()
    {
        skipDuplicateTestCoverage("testLikePredicate");
    }

    @Override
    public void testMaterializedView()
    {
        skipDuplicateTestCoverage("testMaterializedView");
    }

    @Override
    public void testMaterializedViewAllTypes()
    {
        skipDuplicateTestCoverage("testMaterializedViewAllTypes");
    }

    @Override
    public void testMaterializedViewBaseTableGone(boolean arg0)
    {
        skipDuplicateTestCoverage("testMaterializedViewBaseTableGone", boolean.class);
    }

    @Override
    public void testMaterializedViewColumnName(String arg0)
    {
        skipDuplicateTestCoverage("testMaterializedViewColumnName", String.class);
    }

    @Override
    public void testMaterializedViewGracePeriod()
    {
        skipDuplicateTestCoverage("testMaterializedViewGracePeriod");
    }

    @Override
    public void testMergeAllColumnsReversed()
    {
        skipDuplicateTestCoverage("testMergeAllColumnsReversed");
    }

    @Override
    public void testMergeAllColumnsUpdated()
    {
        skipDuplicateTestCoverage("testMergeAllColumnsUpdated");
    }

    @Override
    public void testMergeAllInserts()
    {
        skipDuplicateTestCoverage("testMergeAllInserts");
    }

    @Override
    public void testMergeAllMatchesDeleted()
    {
        skipDuplicateTestCoverage("testMergeAllMatchesDeleted");
    }

    @Override
    public void testMergeCasts()
    {
        skipDuplicateTestCoverage("testMergeCasts");
    }

    @Override
    public void testMergeDeleteWithCTAS()
    {
        skipDuplicateTestCoverage("testMergeDeleteWithCTAS");
    }

    @Override
    public void testMergeFalseJoinCondition()
    {
        skipDuplicateTestCoverage("testMergeFalseJoinCondition");
    }

    @Override
    public void testMergeFruits()
    {
        skipDuplicateTestCoverage("testMergeFruits");
    }

    @Override
    public void testMergeLarge()
    {
        skipDuplicateTestCoverage("testMergeLarge");
    }

    @Override
    public void testMergeMultipleOperations()
    {
        skipDuplicateTestCoverage("testMergeMultipleOperations");
    }

    @Override
    public void testMergeMultipleRowsMatchFails()
    {
        skipDuplicateTestCoverage("testMergeMultipleRowsMatchFails");
    }

    @Override
    public void testMergeNonNullableColumns()
    {
        skipDuplicateTestCoverage("testMergeNonNullableColumns");
    }

    @Override
    public void testMergeQueryWithStrangeCapitalization()
    {
        skipDuplicateTestCoverage("testMergeQueryWithStrangeCapitalization");
    }

    @Override
    public void testMergeSimpleQuery()
    {
        skipDuplicateTestCoverage("testMergeSimpleQuery");
    }

    @Override
    public void testMergeSimpleSelect()
    {
        skipDuplicateTestCoverage("testMergeSimpleSelect");
    }

    @Override
    public void testMergeSubqueries()
    {
        skipDuplicateTestCoverage("testMergeSubqueries");
    }

    @Override
    public void testMergeWithSimplifiedUnpredictablePredicates()
    {
        skipDuplicateTestCoverage("testMergeWithSimplifiedUnpredictablePredicates");
    }

    @Override
    public void testMergeWithUnpredictablePredicates()
    {
        skipDuplicateTestCoverage("testMergeWithUnpredictablePredicates");
    }

    @Override
    public void testMergeWithoutTablesAliases()
    {
        skipDuplicateTestCoverage("testMergeWithoutTablesAliases");
    }

    @Override
    public void testMultipleRangesPredicate()
    {
        skipDuplicateTestCoverage("testMultipleRangesPredicate");
    }

    @Override
    public void testNoDataSystemTable()
    {
        skipDuplicateTestCoverage("testNoDataSystemTable");
    }

    @Override
    public void testPotentialDuplicateDereferencePushdown()
    {
        skipDuplicateTestCoverage("testPotentialDuplicateDereferencePushdown");
    }

    @Override
    public void testPredicateOnRowTypeField()
    {
        skipDuplicateTestCoverage("testPredicateOnRowTypeField");
    }

    @Override
    public void testPredicateReflectedInExplain()
    {
        skipDuplicateTestCoverage("testPredicateReflectedInExplain");
    }

    @Override
    public void testProjectionPushdown()
    {
        skipDuplicateTestCoverage("testProjectionPushdown");
    }

    @Override
    public void testProjectionPushdownMultipleRows()
    {
        skipDuplicateTestCoverage("testProjectionPushdownMultipleRows");
    }

    @Override
    public void testProjectionPushdownPhysicalInputSize()
    {
        skipDuplicateTestCoverage("testProjectionPushdownPhysicalInputSize");
    }

    @Override
    public void testProjectionPushdownReadsLessData()
    {
        skipDuplicateTestCoverage("testProjectionPushdownReadsLessData");
    }

    @Override
    public void testProjectionPushdownWithHighlyNestedData()
    {
        skipDuplicateTestCoverage("testProjectionPushdownWithHighlyNestedData");
    }

    @Override
    public void testProjectionWithCaseSensitiveField()
    {
        skipDuplicateTestCoverage("testProjectionWithCaseSensitiveField");
    }

    @Override
    public void testQueryLoggingCount()
    {
        skipDuplicateTestCoverage("testQueryLoggingCount");
    }

    @Override
    public void testRangePredicate()
    {
        skipDuplicateTestCoverage("testRangePredicate");
    }

    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        skipDuplicateTestCoverage("testReadMetadataWithRelationsConcurrentModifications");
    }

    @Override
    public void testRenameColumnName(String arg0)
    {
        skipDuplicateTestCoverage("testRenameColumnName", String.class);
    }

    @Override
    public void testRenameColumnWithComment()
    {
        skipDuplicateTestCoverage("testRenameColumnWithComment");
    }

    @Override
    public void testRenameMaterializedView()
    {
        skipDuplicateTestCoverage("testRenameMaterializedView");
    }

    @Override
    public void testRenameRowField()
    {
        skipDuplicateTestCoverage("testRenameRowField");
    }

    @Override
    public void testRenameRowFieldCaseSensitivity()
    {
        skipDuplicateTestCoverage("testRenameRowFieldCaseSensitivity");
    }

    @Override
    public void testRenameSchema()
    {
        skipDuplicateTestCoverage("testRenameSchema");
    }

    @Override
    public void testRenameSchemaToLongName()
    {
        skipDuplicateTestCoverage("testRenameSchemaToLongName");
    }

    @Override
    public void testRenameTable()
    {
        skipDuplicateTestCoverage("testRenameTable");
    }

    @Override
    public void testRenameTableAcrossSchema()
    {
        skipDuplicateTestCoverage("testRenameTableAcrossSchema");
    }

    @Override
    public void testRenameTableToLongTableName()
    {
        skipDuplicateTestCoverage("testRenameTableToLongTableName");
    }

    @Override
    public void testRenameTableToUnqualifiedPreservesSchema()
    {
        skipDuplicateTestCoverage("testRenameTableToUnqualifiedPreservesSchema");
    }

    @Override
    public void testRollback()
    {
        skipDuplicateTestCoverage("testRollback");
    }

    @Override
    public void testSelectAfterInsertInTransaction()
    {
        skipDuplicateTestCoverage("testSelectAfterInsertInTransaction");
    }

    @Override
    public void testSelectAll()
    {
        skipDuplicateTestCoverage("testSelectAll");
    }

    @Override
    public void testSelectInTransaction()
    {
        skipDuplicateTestCoverage("testSelectInTransaction");
    }

    @Override
    public void testSelectInformationSchemaColumns()
    {
        skipDuplicateTestCoverage("testSelectInformationSchemaColumns");
    }

    @Override
    public void testSelectInformationSchemaTables()
    {
        skipDuplicateTestCoverage("testSelectInformationSchemaTables");
    }

    @Override
    public void testSelectVersionOfNonExistentTable()
    {
        skipDuplicateTestCoverage("testSelectVersionOfNonExistentTable");
    }

    @Override
    public void testSetColumnIncompatibleType()
    {
        skipDuplicateTestCoverage("testSetColumnIncompatibleType");
    }

    @Override
    public void testSetColumnOutOfRangeType()
    {
        skipDuplicateTestCoverage("testSetColumnOutOfRangeType");
    }

    @Override
    public void testSetColumnType()
    {
        skipDuplicateTestCoverage("testSetColumnType");
    }

    @Override
    public void testSetColumnTypeWithComment()
    {
        skipDuplicateTestCoverage("testSetColumnTypeWithComment");
    }

    @Override
    public void testSetColumnTypeWithDefaultColumn()
    {
        skipDuplicateTestCoverage("testSetColumnTypeWithDefaultColumn");
    }

    @Override
    public void testSetColumnTypeWithNotNull()
    {
        skipDuplicateTestCoverage("testSetColumnTypeWithNotNull");
    }

    @Override
    public void testSetColumnTypes(BaseConnectorTest.SetColumnTypeSetup arg0)
    {
        skipDuplicateTestCoverage("testSetColumnTypes", BaseConnectorTest.SetColumnTypeSetup.class);
    }

    @Override
    public void testSetFieldIncompatibleType()
    {
        skipDuplicateTestCoverage("testSetFieldIncompatibleType");
    }

    @Override
    public void testSetFieldOutOfRangeType()
    {
        skipDuplicateTestCoverage("testSetFieldOutOfRangeType");
    }

    @Override
    public void testSetFieldType()
    {
        skipDuplicateTestCoverage("testSetFieldType");
    }

    @Override
    public void testSetFieldTypeCaseSensitivity()
    {
        skipDuplicateTestCoverage("testSetFieldTypeCaseSensitivity");
    }

    @Override
    public void testSetFieldTypeWithComment()
    {
        skipDuplicateTestCoverage("testSetFieldTypeWithComment");
    }

    @Override
    public void testSetFieldTypeWithNotNull()
    {
        skipDuplicateTestCoverage("testSetFieldTypeWithNotNull");
    }

    @Override
    public void testSetFieldTypes(BaseConnectorTest.SetColumnTypeSetup arg0)
    {
        skipDuplicateTestCoverage("testSetFieldTypes", BaseConnectorTest.SetColumnTypeSetup.class);
    }

    @Override
    public void testShowCreateInformationSchema()
    {
        skipDuplicateTestCoverage("testShowCreateInformationSchema");
    }

    @Override
    public void testShowCreateInformationSchemaTable()
    {
        skipDuplicateTestCoverage("testShowCreateInformationSchemaTable");
    }

    @Override
    public void testShowCreateView()
    {
        skipDuplicateTestCoverage("testShowCreateView");
    }

    @Override
    public void testShowInformationSchemaTables()
    {
        skipDuplicateTestCoverage("testShowInformationSchemaTables");
    }

    @Override
    public void testShowSchemasFromOther()
    {
        skipDuplicateTestCoverage("testShowSchemasFromOther");
    }

    @Override
    public void testSortItemsReflectedInExplain()
    {
        skipDuplicateTestCoverage("testSortItemsReflectedInExplain");
    }

    @Override
    public void testSymbolAliasing()
    {
        skipDuplicateTestCoverage("testSymbolAliasing");
    }

    @Override
    public void testTableSampleSystem()
    {
        skipDuplicateTestCoverage("testTableSampleSystem");
    }

    @Override
    public void testTableSampleWithFiltering()
    {
        skipDuplicateTestCoverage("testTableSampleWithFiltering");
    }

    @Override
    public void testTruncateTable()
    {
        skipDuplicateTestCoverage("testTruncateTable");
    }

    @Override
    public void testTrySelectTableVersion()
    {
        skipDuplicateTestCoverage("testTrySelectTableVersion");
    }

    @Override
    public void testUpdateNotNullColumn()
    {
        skipDuplicateTestCoverage("testUpdateNotNullColumn");
    }

    @Override
    public void testVarcharCastToDateInPredicate()
    {
        skipDuplicateTestCoverage("testVarcharCastToDateInPredicate");
    }

    @Override
    public void testVarcharCharComparison()
    {
        skipDuplicateTestCoverage("testVarcharCharComparison");
    }

    @Override
    public void testView()
    {
        skipDuplicateTestCoverage("testView");
    }

    @Override
    public void testViewAndMaterializedViewTogether()
    {
        skipDuplicateTestCoverage("testViewAndMaterializedViewTogether");
    }

    @Override
    public void testViewCaseSensitivity()
    {
        skipDuplicateTestCoverage("testViewCaseSensitivity");
    }

    @Override
    public void testViewMetadata(String arg0, String arg1)
    {
        skipDuplicateTestCoverage("testViewMetadata", String.class, String.class);
    }

    @Override
    public void testWriteNotAllowedInTransaction()
    {
        skipDuplicateTestCoverage("testWriteNotAllowedInTransaction");
    }

    @Override
    public void testWrittenDataSize()
    {
        skipDuplicateTestCoverage("testWrittenDataSize");
    }

    @Override
    public void testWrittenStats()
    {
        skipDuplicateTestCoverage("testWrittenStats");
    }

    @Override
    public void verifySupportsDeleteDeclaration()
    {
        skipDuplicateTestCoverage("verifySupportsDeleteDeclaration");
    }

    @Override
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        skipDuplicateTestCoverage("verifySupportsRowLevelDeleteDeclaration");
    }
}
