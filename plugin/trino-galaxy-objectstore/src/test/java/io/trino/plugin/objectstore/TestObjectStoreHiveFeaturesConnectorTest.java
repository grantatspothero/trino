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
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

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
        ObjectStoreConnector objectStoreConnector = transaction(getDistributedQueryRunner().getTransactionManager(), getDistributedQueryRunner().getMetadata(), getDistributedQueryRunner().getAccessControl())
                .execute(getSession(), transactionSession -> (ObjectStoreConnector) getDistributedQueryRunner().getCoordinator().getConnector(transactionSession, catalog));
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

    @Test
    @Override
    public void testCreateSchemaWithAuthorizationForUser()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testCreateSchemaWithAuthorizationForRole()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testSchemaAuthorization()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testSchemaAuthorizationForUser()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testSchemaAuthorizationForRole()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testTableAuthorization()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testTableAuthorizationForRole()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testViewAuthorization()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testViewAuthorizationForRole()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testViewAuthorizationSecurityDefiner()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testViewAuthorizationSecurityInvoker()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testShowTablePrivileges()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testShowColumnMetadata()
    {
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testCurrentUserInView()
    {
        // The test merit is not Galaxy specific, but the test structure is.
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        // The test merit is not Galaxy specific, but the test structure is.
        skipAuthorizationRelatedTest();
    }

    @Test
    @Override
    public void testExtraProperties()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testExtraProperties)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Test
    @Override
    public void testExtraPropertiesWithCtas()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testExtraPropertiesWithCtas)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Test
    @Override
    public void testShowCreateWithExtraProperties()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testShowCreateWithExtraProperties)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Test
    @Override
    public void testDuplicateExtraProperties()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testDuplicateExtraProperties)
                .hasMessageContaining("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Test
    @Override
    public void testOverwriteExistingPropertyWithExtraProperties()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testOverwriteExistingPropertyWithExtraProperties)
                .hasMessageContaining("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Test
    @Override
    public void testNullExtraProperty()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testNullExtraProperty)
                .hasMessageContaining("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Test
    @Override
    public void testCollidingMixedCaseProperty()
    {
        // Arbitrary extra_properties currently not exposed in Galaxy
        assertThatThrownBy(super::testCollidingMixedCaseProperty)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' table property 'extra_properties' does not exist");
    }

    @Test
    @Override
    public void testCreateAndInsert()
    {
        assertThatThrownBy(super::testCreateAndInsert)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog only supports writes using autocommit: hive");
    }

    @Test
    @Override
    public void testDeleteAndInsert()
    {
        assertThatThrownBy(super::testDeleteAndInsert)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog only supports writes using autocommit: hive");
    }

    @Test
    @Override
    public void testInsertIntoPartitionedBucketedTableFromBucketedTable()
    {
        assertThatThrownBy(super::testInsertIntoPartitionedBucketedTableFromBucketedTable)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog only supports writes using autocommit: hive");
    }

    @Test
    @Override
    public void testMismatchedBucketing()
    {
        assertThatThrownBy(super::testMismatchedBucketing)
                .hasStackTraceContaining("Not in a transaction");
    }

    @Test
    @Override
    public void testOptimize()
    {
        assertThatThrownBy(super::testOptimize)
                .hasMessageContaining("Executing OPTIMIZE on Hive tables is not supported");
    }

    @Test
    @Override
    public void testOptimizeWithPartitioning()
    {
        assertThatThrownBy(super::testOptimizeWithPartitioning)
                .hasMessageContaining("Executing OPTIMIZE on Hive tables is not supported");
    }

    @Test
    @Override
    public void testOptimizeWithBucketing()
    {
        assertThatThrownBy(super::testOptimizeWithBucketing)
                .hasMessageContaining("Executing OPTIMIZE on Hive tables is not supported");
    }

    @Test
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
        abort("Test is disabled. Hive authorization & roles work differently in Galaxy");
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
    public void testAddAndDropColumnName()
    {
        skipDuplicateTestCoverage("testAddAndDropColumnName");
    }

    @Test
    @Override
    public void testAddColumn()
    {
        skipDuplicateTestCoverage("testAddColumn");
    }

    @Test
    @Override
    public void testAddColumnWithComment()
    {
        skipDuplicateTestCoverage("testAddColumnWithComment");
    }

    @Test
    @Override
    public void testAddColumnWithCommentSpecialCharacter()
    {
        skipDuplicateTestCoverage("testAddColumnWithCommentSpecialCharacter");
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
    public void testAggregationOverUnknown()
    {
        skipDuplicateTestCoverage("testAggregationOverUnknown");
    }

    @Test
    @Override
    public void testAlterTableAddLongColumnName()
    {
        skipDuplicateTestCoverage("testAlterTableAddLongColumnName");
    }

    @Test
    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        skipDuplicateTestCoverage("testAlterTableRenameColumnToLongName");
    }

    @Test
    @Override
    public void testArithmeticNegation()
    {
        skipDuplicateTestCoverage("testArithmeticNegation");
    }

    @Test
    @Override
    public void testCaseSensitiveDataMapping()
    {
        skipDuplicateTestCoverage("testCaseSensitiveDataMapping");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        skipDuplicateTestCoverage("testCharVarcharComparison");
    }

    @Test
    @Override
    public void testColumnCommentMaterializedView()
    {
        skipDuplicateTestCoverage("testColumnCommentMaterializedView");
    }

    @Test
    @Override
    public void testColumnName()
    {
        skipDuplicateTestCoverage("testColumnName");
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
    public void testCommentColumnName()
    {
        skipDuplicateTestCoverage("testCommentColumnName");
    }

    @Test
    @Override
    public void testCommentColumnSpecialCharacter()
    {
        skipDuplicateTestCoverage("testCommentColumnSpecialCharacter");
    }

    @Test
    @Override
    public void testCommentTable()
    {
        skipDuplicateTestCoverage("testCommentTable");
    }

    @Test
    @Override
    public void testCommentTableSpecialCharacter()
    {
        skipDuplicateTestCoverage("testCommentTableSpecialCharacter");
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
    public void testComplexQuery()
    {
        skipDuplicateTestCoverage("testComplexQuery");
    }

    @Test
    @Override
    public void testConcurrentScans()
    {
        skipDuplicateTestCoverage("testConcurrentScans");
    }

    @Test
    @Override
    public void testCountAll()
    {
        skipDuplicateTestCoverage("testCountAll");
    }

    @Test
    @Override
    public void testCountColumn()
    {
        skipDuplicateTestCoverage("testCountColumn");
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
    public void testCreateTableAsSelectWithTableCommentSpecialCharacter()
    {
        skipDuplicateTestCoverage("testCreateTableAsSelectWithTableCommentSpecialCharacter");
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
    public void testCreateTableWithColumnCommentSpecialCharacter()
    {
        skipDuplicateTestCoverage("testCreateTableWithColumnCommentSpecialCharacter");
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
    public void testCreateTableWithTableCommentSpecialCharacter()
    {
        skipDuplicateTestCoverage("testCreateTableWithTableCommentSpecialCharacter");
    }

    @Test
    @Override
    public void testCreateViewSchemaNotFound()
    {
        skipDuplicateTestCoverage("testCreateViewSchemaNotFound");
    }

    @Test
    @Override
    public void testDataMappingSmokeTest()
    {
        skipDuplicateTestCoverage("testDataMappingSmokeTest");
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        skipDuplicateTestCoverage("testDateYearOfEraPredicate");
    }

    @Test
    @Override
    public void testDeleteAllDataFromTable()
    {
        skipDuplicateTestCoverage("testDeleteAllDataFromTable");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        skipDuplicateTestCoverage("testDescribeTable");
    }

    @Test
    @Override
    public void testDistinct()
    {
        skipDuplicateTestCoverage("testDistinct");
    }

    @Test
    @Override
    public void testDistinctHaving()
    {
        skipDuplicateTestCoverage("testDistinctHaving");
    }

    @Test
    @Override
    public void testDistinctLimit()
    {
        skipDuplicateTestCoverage("testDistinctLimit");
    }

    @Test
    @Override
    public void testDistinctMultipleFields()
    {
        skipDuplicateTestCoverage("testDistinctMultipleFields");
    }

    @Test
    @Override
    public void testDistinctWithOrderBy()
    {
        skipDuplicateTestCoverage("testDistinctWithOrderBy");
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
    public void testDropNonEmptySchemaWithTable()
    {
        skipDuplicateTestCoverage("testDropNonEmptySchemaWithTable");
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
    public void testFilterPushdownWithAggregation()
    {
        skipDuplicateTestCoverage("testFilterPushdownWithAggregation");
    }

    @Test
    @Override
    public void testIn()
    {
        skipDuplicateTestCoverage("testIn");
    }

    @Test
    @Override
    public void testInListPredicate()
    {
        skipDuplicateTestCoverage("testInListPredicate");
    }

    @Test
    @Override
    public void testInformationSchemaFiltering()
    {
        skipDuplicateTestCoverage("testInformationSchemaFiltering");
    }

    @Test
    @Override
    public void testInformationSchemaUppercaseName()
    {
        skipDuplicateTestCoverage("testInformationSchemaUppercaseName");
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
    public void testInsertSameValues()
    {
        skipDuplicateTestCoverage("testInsertSameValues");
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
    public void testJoinWithEmptySides()
    {
        skipDuplicateTestCoverage("testJoinWithEmptySides");
    }

    @Test
    @Override
    public void testLargeIn()
    {
        skipDuplicateTestCoverage("testLargeIn");
    }

    @Test
    @Override
    public void testLikePredicate()
    {
        skipDuplicateTestCoverage("testLikePredicate");
    }

    @Test
    @Override
    public void testLimit()
    {
        skipDuplicateTestCoverage("testLimit");
    }

    @Test
    @Override
    public void testLimitInInlineView()
    {
        skipDuplicateTestCoverage("testLimitInInlineView");
    }

    @Test
    @Override
    public void testLimitMax()
    {
        skipDuplicateTestCoverage("testLimitMax");
    }

    @Test
    @Override
    public void testLimitWithAggregation()
    {
        skipDuplicateTestCoverage("testLimitWithAggregation");
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
    public void testMaterializedViewBaseTableGone()
    {
        skipDuplicateTestCoverage("testMaterializedViewBaseTableGone");
    }

    @Test
    @Override
    public void testMaterializedViewColumnName()
    {
        skipDuplicateTestCoverage("testMaterializedViewColumnName");
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
    public void testPredicate()
    {
        skipDuplicateTestCoverage("testPredicate");
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
    public void testRenameColumnName()
    {
        skipDuplicateTestCoverage("testRenameColumnName");
    }

    @Test
    @Override
    public void testRenameColumnWithComment()
    {
        skipDuplicateTestCoverage("testRenameColumnWithComment");
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
    public void testRepeatedAggregations()
    {
        skipDuplicateTestCoverage("testRepeatedAggregations");
    }

    @Test
    @Override
    public void testRollback()
    {
        skipDuplicateTestCoverage("testRollback");
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
    public void testSelectWithComparison()
    {
        skipDuplicateTestCoverage("testSelectWithComparison");
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
    public void testSetColumnTypes()
    {
        skipDuplicateTestCoverage("testSetColumnTypes");
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
    public void testSetFieldTypes()
    {
        skipDuplicateTestCoverage("testSetFieldTypes");
    }

    @Test
    @Override
    public void testShowColumns()
    {
        skipDuplicateTestCoverage("testShowColumns");
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
    public void testShowSchemas()
    {
        skipDuplicateTestCoverage("testShowSchemas");
    }

    @Test
    @Override
    public void testShowSchemasFrom()
    {
        skipDuplicateTestCoverage("testShowSchemasFrom");
    }

    @Test
    @Override
    public void testShowSchemasFromOther()
    {
        skipDuplicateTestCoverage("testShowSchemasFromOther");
    }

    @Test
    @Override
    public void testShowSchemasLike()
    {
        skipDuplicateTestCoverage("testShowSchemasLike");
    }

    @Test
    @Override
    public void testShowSchemasLikeWithEscape()
    {
        skipDuplicateTestCoverage("testShowSchemasLikeWithEscape");
    }

    @Test
    @Override
    public void testShowTables()
    {
        skipDuplicateTestCoverage("testShowTables");
    }

    @Test
    @Override
    public void testShowTablesLike()
    {
        skipDuplicateTestCoverage("testShowTablesLike");
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
    public void testTableSampleBernoulli()
    {
        skipDuplicateTestCoverage("testTableSampleBernoulli");
    }

    @Test
    @Override
    public void testTableSampleBernoulliBoundaryValues()
    {
        skipDuplicateTestCoverage("testTableSampleBernoulliBoundaryValues");
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
    public void testTopN()
    {
        skipDuplicateTestCoverage("testTopN");
    }

    @Test
    @Override
    public void testTopNByMultipleFields()
    {
        skipDuplicateTestCoverage("testTopNByMultipleFields");
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
    public void testUnionAllAboveBroadcastJoin()
    {
        skipDuplicateTestCoverage("testUnionAllAboveBroadcastJoin");
    }

    @Test
    @Override
    public void testUpdateNotNullColumn()
    {
        skipDuplicateTestCoverage("testUpdateNotNullColumn");
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
    public void testViewMetadata()
    {
        skipDuplicateTestCoverage("testViewMetadata");
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
}
