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
import io.trino.plugin.iceberg.BaseIcebergConnectorTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.initializeTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.fail;

/**
 * Tests ObjectStore connector with Iceberg backend, exercising all
 * iceberg-specific tests inherited from {@link BaseIcebergConnectorTest}.
 *
 * @see TestObjectStoreIcebergConnectorTest
 */
public class TestObjectStoreIcebergFeaturesConnectorTest
        extends BaseIcebergConnectorTest
{
    protected TestObjectStoreIcebergFeaturesConnectorTest()
    {
        super(new IcebergConfig().getFileFormat());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String catalog = "iceberg";
        String schema = "tpch";
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(catalog)
                                .setSchema(schema)
                                .build())
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", Map.of());

            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            String metastoreDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toString();
            queryRunner.createCatalog(catalog, "galaxy_objectstore", ImmutableMap.<String, String>builder()
                    // Hive
                    .put("HIVE__hive.metastore", "file")
                    .put("HIVE__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("HIVE__galaxy.location-security.enabled", "false")
                    .put("HIVE__galaxy.account-url", "https://localhost:1234")
                    // Iceberg
                    .put("ICEBERG__iceberg.catalog.type", "TESTING_FILE_METASTORE")
                    .put("ICEBERG__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("ICEBERG__galaxy.location-security.enabled", "false")
                    .put("ICEBERG__galaxy.account-url", "https://localhost:1234")
                    // Delta
                    .put("DELTA__hive.metastore", "file")
                    .put("DELTA__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("DELTA__galaxy.location-security.enabled", "false")
                    .put("DELTA__galaxy.account-url", "https://localhost:1234")
                    // Hudi
                    .put("HUDI__hive.metastore", "file")
                    .put("HUDI__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("HUDI__galaxy.location-security.enabled", "false")
                    .put("HUDI__galaxy.account-url", "https://localhost:1234")
                    // ObjectStore
                    .put("OBJECTSTORE__object-store.table-type", TableType.ICEBERG.name())
                    .put("OBJECTSTORE__galaxy.location-security.enabled", "false")
                    .put("OBJECTSTORE__galaxy.account-url", "https://localhost:1234")
                    .buildOrThrow());

            queryRunner.execute("CREATE SCHEMA %s.%s".formatted(catalog, schema));
            initializeTpchTables(queryRunner, REQUIRED_TPCH_TABLES);

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

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        checkState(format == IcebergFileFormat.ORC, "The logic here is appropriate for ORC, got %s", format);
        return !typeName.equalsIgnoreCase("varbinary") &&
                !typeName.equalsIgnoreCase("uuid");
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        checkState(format == IcebergFileFormat.ORC, "The logic here is appropriate for ORC, got %s", format);
        return !typeName.equalsIgnoreCase("varbinary");
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return checkOrcFileSorting(path, sortColumnName);
    }

    @BeforeMethod(alwaysRun = true)
    public void preventDuplicatedTestCoverage(Method testMethod)
    {
        Class<?> declaringClass = testMethod.getDeclaringClass();
        if (declaringClass.isAssignableFrom(BaseConnectorTest.class) && !isOverridden(testMethod, getClass())) {
            fail("The %s test is covered by %s, no need to run it again in %s. You can use main() to generate overrides".formatted(
                    testMethod.getName(),
                    TestObjectStoreIcebergConnectorTest.class.getSimpleName(),
                    getClass().getSimpleName()));
        }
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

    @Override
    public void testSerializableReadIsolation()
    {
        // HiveConnector has READ_UNCOMMITTED. When opening transaction in ObjectStore we don't know yet we which tables we will read from.
        assertThatThrownBy(super::testSerializableReadIsolation)
                .cause()
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Connector supported isolation level READ UNCOMMITTED does not meet requested isolation level READ COMMITTED");
    }

    private void skipDuplicateTestCoverage(String methodName, Class<?>... args)
    {
        try {
            Method ignored = getClass().getDeclaredMethod(methodName, args); // validate we have the override
            if (isTestSpecializedForIceberg(methodName, args)) {
                fail("Method %s(%s) became overridden and should no longer be skipped in %s".formatted(methodName, Arrays.toString(args), getClass()));
            }
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }

        throw new SkipException("This method is probably run in %s".formatted(TestObjectStoreIcebergConnectorTest.class.getSimpleName()));
    }

    private boolean isTestSpecializedForIceberg(String methodName, Class<?>... args)
    {
        try {
            Method overridden = getClass().getSuperclass().getMethod(methodName, args);
            return !overridden.getDeclaringClass().isAssignableFrom(BaseConnectorTest.class);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args)
    {
        // Generate overrides

        Stream.of(BaseConnectorTest.class.getMethods())
                .filter(method -> method.isAnnotationPresent(Test.class) && !isOverridden(method, TestObjectStoreIcebergFeaturesConnectorTest.class))
                .sorted(Comparator.comparing(Method::getName))
                .forEachOrdered(method -> {
                    System.out.printf(
                            """
                                    @Override
                                    public void %s(%s)
                                    {
                                        skipDuplicateTestCoverage("%1$s"%s);
                                    }
                                    \n""", method.getName(),
                            IntStream.range(0, method.getParameterTypes().length)
                                    .mapToObj(i -> "%s arg%s".formatted(formatClassName(method.getParameterTypes()[i]), i))
                                    .collect(joining(", ")),
                            Stream.of(method.getParameterTypes())
                                    .map(clazz -> ", %s.class".formatted(formatClassName(clazz)))
                                    .collect(joining()));
                });
    }

    private static boolean isOverridden(Method method, Class<?> byClazz)
    {
        checkArgument(
                method.getDeclaringClass() != byClazz && method.getDeclaringClass().isAssignableFrom(byClazz),
                "%s is not a subclass of %s which declares %s",
                byClazz,
                method.getDeclaringClass(),
                method);

        for (Class<?> clazz = byClazz; clazz != method.getDeclaringClass(); clazz = clazz.getSuperclass()) {
            try {
                Method ignored = clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
                return true;
            }
            catch (NoSuchMethodException ignore) {
                // continue
            }
        }
        return false;
    }

    private static String formatClassName(Class<?> clazz)
    {
        String className = clazz.getSimpleName();
        // IntelliJ does not auto-import nested names, so use the top-level class name
        for (Class<?> enclosingClass = clazz.getEnclosingClass(); enclosingClass != null; enclosingClass = enclosingClass.getEnclosingClass()) {
            //noinspection StringConcatenationInLoop
            className = enclosingClass.getSimpleName() + "." + className;
        }
        return className;
    }

    /////// ----------------------------------------- please put generated code below this line ----------------------------------------- ///////
    /////// ----------------------------------------- please put generated code also below this line ------------------------------------ ///////
    /////// ----------------------------------------- please put generated code below this line as well --------------------------------- ///////

    @Override
    public void isReportingWrittenBytesSupported()
    {
        skipDuplicateTestCoverage("isReportingWrittenBytesSupported");
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
    public void testAggregation()
    {
        skipDuplicateTestCoverage("testAggregation");
    }

    @Override
    public void testAggregationOverUnknown()
    {
        skipDuplicateTestCoverage("testAggregationOverUnknown");
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
    public void testArithmeticNegation()
    {
        skipDuplicateTestCoverage("testArithmeticNegation");
    }

    @Override
    public void testCaseSensitiveDataMapping(BaseConnectorTest.DataMappingTestSetup arg0)
    {
        skipDuplicateTestCoverage("testCaseSensitiveDataMapping", BaseConnectorTest.DataMappingTestSetup.class);
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
    public void testComplexQuery()
    {
        skipDuplicateTestCoverage("testComplexQuery");
    }

    @Override
    public void testConcurrentScans()
    {
        skipDuplicateTestCoverage("testConcurrentScans");
    }

    @Override
    public void testCountAll()
    {
        skipDuplicateTestCoverage("testCountAll");
    }

    @Override
    public void testCountColumn()
    {
        skipDuplicateTestCoverage("testCountColumn");
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
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        skipDuplicateTestCoverage("testCreateSchemaWithNonLowercaseOwnerName");
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
    public void testDelete()
    {
        skipDuplicateTestCoverage("testDelete");
    }

    @Override
    public void testDeleteAllDataFromTable()
    {
        skipDuplicateTestCoverage("testDeleteAllDataFromTable");
    }

    @Override
    public void testDeleteWithComplexPredicate()
    {
        skipDuplicateTestCoverage("testDeleteWithComplexPredicate");
    }

    @Override
    public void testDeleteWithLike()
    {
        skipDuplicateTestCoverage("testDeleteWithLike");
    }

    @Override
    public void testDeleteWithSemiJoin()
    {
        skipDuplicateTestCoverage("testDeleteWithSemiJoin");
    }

    @Override
    public void testDeleteWithSubquery()
    {
        skipDuplicateTestCoverage("testDeleteWithSubquery");
    }

    @Override
    public void testDeleteWithVarcharPredicate()
    {
        skipDuplicateTestCoverage("testDeleteWithVarcharPredicate");
    }

    @Override
    public void testDescribeTable()
    {
        skipDuplicateTestCoverage("testDescribeTable");
    }

    @Override
    public void testDistinct()
    {
        skipDuplicateTestCoverage("testDistinct");
    }

    @Override
    public void testDistinctHaving()
    {
        skipDuplicateTestCoverage("testDistinctHaving");
    }

    @Override
    public void testDistinctLimit()
    {
        skipDuplicateTestCoverage("testDistinctLimit");
    }

    @Override
    public void testDistinctMultipleFields()
    {
        skipDuplicateTestCoverage("testDistinctMultipleFields");
    }

    @Override
    public void testDistinctWithOrderBy()
    {
        skipDuplicateTestCoverage("testDistinctWithOrderBy");
    }

    @Override
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        skipDuplicateTestCoverage("testDropAmbiguousRowFieldCaseSensitivity");
    }

    @Override
    public void testDropAndAddColumnWithSameName()
    {
        skipDuplicateTestCoverage("testDropAndAddColumnWithSameName");
    }

    @Override
    public void testDropColumn()
    {
        skipDuplicateTestCoverage("testDropColumn");
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
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        skipDuplicateTestCoverage("testExplainAnalyzeWithDeleteWithSubquery");
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
    public void testFilterPushdownWithAggregation()
    {
        skipDuplicateTestCoverage("testFilterPushdownWithAggregation");
    }

    @Override
    public void testIn()
    {
        skipDuplicateTestCoverage("testIn");
    }

    @Override
    public void testInListPredicate()
    {
        skipDuplicateTestCoverage("testInListPredicate");
    }

    @Override
    public void testInformationSchemaFiltering()
    {
        skipDuplicateTestCoverage("testInformationSchemaFiltering");
    }

    @Override
    public void testInformationSchemaUppercaseName()
    {
        skipDuplicateTestCoverage("testInformationSchemaUppercaseName");
    }

    @Override
    public void testInsert()
    {
        skipDuplicateTestCoverage("testInsert");
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
    public void testInsertHighestUnicodeCharacter()
    {
        skipDuplicateTestCoverage("testInsertHighestUnicodeCharacter");
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
    public void testInsertUnicode()
    {
        skipDuplicateTestCoverage("testInsertUnicode");
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
    public void testLargeIn(int arg0)
    {
        skipDuplicateTestCoverage("testLargeIn", int.class);
    }

    @Override
    public void testLikePredicate()
    {
        skipDuplicateTestCoverage("testLikePredicate");
    }

    @Override
    public void testLimit()
    {
        skipDuplicateTestCoverage("testLimit");
    }

    @Override
    public void testLimitInInlineView()
    {
        skipDuplicateTestCoverage("testLimitInInlineView");
    }

    @Override
    public void testLimitMax()
    {
        skipDuplicateTestCoverage("testLimitMax");
    }

    @Override
    public void testLimitWithAggregation()
    {
        skipDuplicateTestCoverage("testLimitWithAggregation");
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
    public void testPredicate()
    {
        skipDuplicateTestCoverage("testPredicate");
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
    public void testRenameColumn()
    {
        skipDuplicateTestCoverage("testRenameColumn");
    }

    @Override
    public void testRenameColumnName(String arg0)
    {
        skipDuplicateTestCoverage("testRenameColumnName", String.class);
    }

    @Override
    public void testRenameMaterializedView()
    {
        skipDuplicateTestCoverage("testRenameMaterializedView");
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
    public void testRepeatedAggregations()
    {
        skipDuplicateTestCoverage("testRepeatedAggregations");
    }

    @Override
    public void testRollback()
    {
        skipDuplicateTestCoverage("testRollback");
    }

    @Override
    public void testRowLevelDelete()
    {
        skipDuplicateTestCoverage("testRowLevelDelete");
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
    public void testSelectWithComparison()
    {
        skipDuplicateTestCoverage("testSelectWithComparison");
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
    public void testShowColumns()
    {
        skipDuplicateTestCoverage("testShowColumns");
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
    public void testShowSchemas()
    {
        skipDuplicateTestCoverage("testShowSchemas");
    }

    @Override
    public void testShowSchemasFrom()
    {
        skipDuplicateTestCoverage("testShowSchemasFrom");
    }

    @Override
    public void testShowSchemasFromOther()
    {
        skipDuplicateTestCoverage("testShowSchemasFromOther");
    }

    @Override
    public void testShowSchemasLike()
    {
        skipDuplicateTestCoverage("testShowSchemasLike");
    }

    @Override
    public void testShowSchemasLikeWithEscape()
    {
        skipDuplicateTestCoverage("testShowSchemasLikeWithEscape");
    }

    @Override
    public void testShowTables()
    {
        skipDuplicateTestCoverage("testShowTables");
    }

    @Override
    public void testShowTablesLike()
    {
        skipDuplicateTestCoverage("testShowTablesLike");
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
    public void testTableSampleBernoulli()
    {
        skipDuplicateTestCoverage("testTableSampleBernoulli");
    }

    @Override
    public void testTableSampleBernoulliBoundaryValues()
    {
        skipDuplicateTestCoverage("testTableSampleBernoulliBoundaryValues");
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
    public void testTopN()
    {
        skipDuplicateTestCoverage("testTopN");
    }

    @Override
    public void testTopNByMultipleFields()
    {
        skipDuplicateTestCoverage("testTopNByMultipleFields");
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
    public void testUpdate()
    {
        skipDuplicateTestCoverage("testUpdate");
    }

    @Override
    public void testUpdateAllValues()
    {
        skipDuplicateTestCoverage("testUpdateAllValues");
    }

    @Override
    public void testUpdateNotNullColumn()
    {
        skipDuplicateTestCoverage("testUpdateNotNullColumn");
    }

    @Override
    public void testUpdateRowConcurrently()
    {
        skipDuplicateTestCoverage("testUpdateRowConcurrently");
    }

    @Override
    public void testUpdateRowType()
    {
        skipDuplicateTestCoverage("testUpdateRowType");
    }

    @Override
    public void testUpdateWithPredicates()
    {
        skipDuplicateTestCoverage("testUpdateWithPredicates");
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
