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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public class TestObjectStoreHiveConnectorTest
        extends BaseObjectStoreConnectorTest
{
    public TestObjectStoreHiveConnectorTest()
    {
        super(HIVE);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        boolean connectorHasBehavior = new GetHiveConnectorTestBehavior().hasBehavior(connectorBehavior);

        switch (connectorBehavior) {
            case SUPPORTS_COMMENT_ON_VIEW: // TODO ObjectStore lacks COMMENT ON VIEW support
            case SUPPORTS_COMMENT_ON_VIEW_COLUMN: // TODO ObjectStore lacks COMMENT ON VIEW column support
            case SUPPORTS_MULTI_STATEMENT_WRITES: // multi-statement transaction support is disabled in ObjectStore
                // when this fails remove the `case` for given flag
                verify(connectorHasBehavior, "Expected support for: %s", connectorBehavior);
                return false;

            // Declared to be had since MERGE-related test cases are overridden
            case SUPPORTS_MERGE:
                return true;

            // ObjectStore adds support for materialized views using Iceberg
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW_GRACE_PERIOD:
            case SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW:
            case SUPPORTS_RENAME_MATERIALIZED_VIEW:
//            case SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS: -- not supported by Iceberg:
                // when this fails remove the `case` for given flag
                verify(!connectorHasBehavior, "Unexpected support for: %s", connectorBehavior);
                return true;

            default:
                // By default, declare all behaviors/features supported by Hive connector
                return connectorHasBehavior;
        }
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        if (typeName.equals("timestamp(6)")) {
            // It's supported depending on hive timestamp precision configuration, so the exception message doesn't match the expected for asUnsupported().
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return switch (columnName) {
            case " aleadingspace" -> "Hive column names must not start with a space: ' aleadingspace'".equals(exception.getMessage());
            case "atrailingspace " -> "Hive column names must not end with a space: 'atrailingspace '".equals(exception.getMessage());
            case "a,comma" -> "Hive column names must not contain commas: 'a,comma'".equals(exception.getMessage());
            default -> false;
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Hive connector does not support column default values");
    }

    @Override
    public void testCreateTableWithLocation()
    {
        assertThatThrownBy(super::testCreateTableWithLocation)
                .hasStackTraceContaining("Table property 'location' not supported for Hive tables");

        assertQueryFails("CREATE TABLE test_location_create (x int) WITH (external_location = 's3://test-bucket/denied')",
                "Access Denied: Role ID r-\\d{10} is not allowed to use location: s3://test-bucket/denied");

        assertQueryFails("CREATE TABLE test_location_create (x int) WITH (external_location = 's3://test-bucket/denied/test_location_create')",
                "Access Denied: Role ID r-\\d{10} is not allowed to use location: s3://test-bucket/denied/test_location_create");
    }

    @Override
    public void testCreateTableAsWithLocation()
    {
        assertThatThrownBy(super::testCreateTableAsWithLocation)
                .hasStackTraceContaining("Table property 'location' not supported for Hive tables");

        assertQueryFails("CREATE TABLE test_location_ctas WITH (external_location = 's3://test-bucket/denied') AS SELECT 123 x",
                "Access Denied: Role ID r-\\d{10} is not allowed to use location: s3://test-bucket/denied");

        assertQueryFails("CREATE TABLE test_location_ctas WITH (external_location = 's3://test-bucket/denied/test_location_ctas') AS SELECT 123 x",
                "Access Denied: Role ID r-\\d{10} is not allowed to use location: s3://test-bucket/denied/test_location_ctas");
    }

    @Test
    public void testRegisterPartitionWithLocation()
    {
        String tableName = "test_register_partition_for_table";
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  dummy_col bigint," +
                "  part varchar)" +
                "WITH (" +
                "  partitioned_by = ARRAY['part'] " +
                ")");

        assertQueryFails("CALL system.register_partition('tpch', '" + tableName + "', ARRAY['part'], ARRAY['first'], 's3://test-bucket/denied')",
                "Access Denied: Role ID r-\\d{10} is not allowed to use location: s3://test-bucket/denied");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testInsertIntoNotNullColumn()
    {
        assertQueryFails(
                "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                "Hive tables do not support NOT NULL columns");
    }

    @Override
    public void testDropAndAddColumnWithSameName()
    {
        // Override because Hive connector can access old data after dropping and adding a column with same name
        assertThatThrownBy(super::testDropAndAddColumnWithSameName)
                .hasMessageContaining("""
                        Actual rows (up to 100 of 1 extra rows shown, 1 rows in total):
                            [1, 2]""");
    }

    @Override
    public void testAddNotNullColumnToEmptyTable()
    {
        // Override because the connector throws a slightly different error message
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_notnull_col", "(a_varchar varchar)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " ADD COLUMN b_varchar varchar NOT NULL",
                    "Hive tables do not support NOT NULL columns");
        }
    }

    @Override
    public void testDelete()
    {
        assertThatThrownBy(super::testDelete)
                .hasStackTraceContaining("Modifying Hive table rows is constrained to deletes of whole partitions");
    }

    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("Modifying Hive table rows is constrained to deletes of whole partitions");
    }

    @Override
    public void testDeleteWithComplexPredicate()
    {
        assertThatThrownBy(super::testDeleteWithComplexPredicate)
                .hasStackTraceContaining("Modifying Hive table rows is constrained to deletes of whole partitions");
    }

    @Override
    public void testMergeDeleteWithCTAS()
    {
        assertThatThrownBy(super::testMergeDeleteWithCTAS)
                .hasStackTraceContaining("Modifying Hive table rows is constrained to deletes of whole partitions");
    }

    @Test
    public void testSortedTable()
    {
        Session withSmallRowGroups = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "orc_optimized_writer_max_stripe_rows", "6")
                .build();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_table",
                "(id INTEGER, name VARCHAR) WITH (bucketed_by = ARRAY[ 'name' ], bucket_count = 2, sorted_by = ARRAY['name'], format = 'ORC')")) {
            String values = " VALUES" +
                    "(5, 'name5')," +
                    "(10, 'name10')," +
                    "(4, 'name4')," +
                    "(11, 'name11')," +
                    "(15, 'name15')," +
                    "(17, 'name17')," +
                    "(20, 'name20')," +
                    "(12, 'name12')," +
                    "(13, 'name13')," +
                    "(1, 'name1')," +
                    "(6, 'name6')," +
                    "(19, 'name19')," +
                    "(18, 'name18')," +
                    "(9, 'name9')," +
                    "(16, 'name16')," +
                    "(8, 'name8')," +
                    "(3, 'name3')," +
                    "(14, 'name14')," +
                    "(7, 'name7')," +
                    "(2, 'name2')";
            assertUpdate(withSmallRowGroups, "INSERT INTO " + table.getName() + values, 20);
            assertQuery("SELECT * FROM " + table.getName(), values.trim());
            TrinoFileSystemFactory fileSystemFactory = getTrinoFileSystemFactory();
            for (Object filePath : computeActual("SELECT DISTINCT \"$path\" FROM " + table.getName()).getOnlyColumnAsSet()) {
                assertTrue(checkOrcFileSorting(fileSystemFactory, Location.of((String) filePath), "name"));
            }
        }
    }

    @Override
    public void testDeleteWithSemiJoin()
    {
        assertThatThrownBy(super::testDeleteWithSemiJoin)
                .hasStackTraceContaining("Modifying Hive table rows is constrained to deletes of whole partitions");
    }

    @Override
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDeleteWithSubquery)
                .hasStackTraceContaining("Modifying Hive table rows is constrained to deletes of whole partitions");
    }

    @Override
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        assertThatThrownBy(super::testExplainAnalyzeWithDeleteWithSubquery)
                .hasStackTraceContaining("Modifying Hive table rows is constrained to deletes of whole partitions");
    }

    @Override
    public void testDeleteWithVarcharPredicate()
    {
        assertThatThrownBy(super::testDeleteWithVarcharPredicate)
                .hasStackTraceContaining("Modifying Hive table rows is constrained to deletes of whole partitions");
    }

    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasStackTraceContaining("Modifying Hive table rows is constrained to deletes of whole partitions");
    }

    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testUpdateRowType()
    {
        assertThatThrownBy(super::testUpdateRowType).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testUpdateAllValues()
    {
        assertThatThrownBy(super::testUpdateAllValues).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testUpdateWithPredicates()
    {
        assertThatThrownBy(super::testUpdateWithPredicates).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeLarge()
    {
        assertThatThrownBy(super::testMergeLarge).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeSimpleSelect()
    {
        assertThatThrownBy(super::testMergeSimpleSelect).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeFruits()
    {
        assertThatThrownBy(super::testMergeFruits).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeMultipleOperations()
    {
        assertThatThrownBy(super::testMergeMultipleOperations).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeSimpleQuery()
    {
        assertThatThrownBy(super::testMergeSimpleQuery).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeAllInserts()
    {
        assertThatThrownBy(super::testMergeAllInserts).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeFalseJoinCondition()
    {
        assertThatThrownBy(super::testMergeFalseJoinCondition).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeAllColumnsUpdated()
    {
        assertThatThrownBy(super::testMergeAllColumnsUpdated).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeAllMatchesDeleted()
    {
        assertThatThrownBy(super::testMergeAllMatchesDeleted).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeMultipleRowsMatchFails()
    {
        assertThatThrownBy(super::testMergeAllMatchesDeleted).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeQueryWithStrangeCapitalization()
    {
        assertThatThrownBy(super::testMergeQueryWithStrangeCapitalization).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeWithoutTablesAliases()
    {
        assertThatThrownBy(super::testMergeWithoutTablesAliases).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeWithUnpredictablePredicates()
    {
        assertThatThrownBy(super::testMergeWithUnpredictablePredicates).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeWithSimplifiedUnpredictablePredicates()
    {
        assertThatThrownBy(super::testMergeWithSimplifiedUnpredictablePredicates).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeCasts()
    {
        assertThatThrownBy(super::testMergeCasts).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    public void testMergeSubqueries()
    {
        assertThatThrownBy(super::testMergeSubqueries).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e).hasMessage("Row-level modifications are not supported for Hive tables");
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertQueryReturns("SHOW CREATE TABLE orders", "" +
                "CREATE TABLE objectstore.tpch.orders (\n" +
                "   orderkey bigint,\n" +
                "   custkey bigint,\n" +
                "   orderstatus varchar(1),\n" +
                "   totalprice double,\n" +
                "   orderdate date,\n" +
                "   orderpriority varchar(15),\n" +
                "   clerk varchar(15),\n" +
                "   shippriority integer,\n" +
                "   comment varchar(79)\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC',\n" +
                "   type = 'HIVE'\n" +
                ")");
    }

    @Override
    public void testIcebergSpecificTableProperty()
    {
        assertThatThrownBy(super::testIcebergSpecificTableProperty)
                .hasMessage("Table property 'partitioning' not supported for Hive tables");
    }

    @Override
    public void testDeltaSpecificTableProperty()
    {
        assertThatThrownBy(super::testDeltaSpecificTableProperty)
                .hasMessage("Table property 'checkpoint_interval' not supported for Hive tables");
    }

    @Override
    protected Double basicTableStatisticsExpectedNdv(int actualNdv)
    {
        return 1.0;
    }

    @Override
    public void testRegisterTableProcedure()
    {
        assertThatThrownBy(super::testRegisterTableProcedure)
                .hasMessage("Unsupported table type");
    }

    @Override
    public void testUnregisterTableProcedure()
    {
        assertThatThrownBy(super::testUnregisterTableProcedure)
                .hasMessage("Unsupported table type");
    }

    @Override
    protected String getTableLocation(String tableName)
    {
        return "s3://test-bucket/tpch/" + tableName;
    }

    @Test
    public void testCreatePartitionedBucketedTable()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + "test_create_partitioned_bucketed_table" + " " +
                "WITH (" +
                "format = 'RCBINARY', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                Session.builder(getSession())
                        .setSystemProperty("task_writer_count", "4")
                        .build(),
                createTable,
                "SELECT count(*) FROM orders");

        getQueryRunner().execute(format("GRANT SELECT ON \"test_create_partitioned_bucketed_table$partitions\" TO ROLE %s", ACCOUNT_ADMIN));
        assertQuery(
                "SELECT count(*) FROM \"test_create_partitioned_bucketed_table$partitions\"",
                "SELECT 3");

        // verify that we create bucket_count files in each partition
        assertEqualsIgnoreOrder(
                computeActual("SELECT orderstatus, COUNT(DISTINCT \"$path\") FROM test_create_partitioned_bucketed_table GROUP BY 1"),
                resultBuilder(getSession(), createVarcharType(1), BIGINT)
                        .row("F", 11L)
                        .row("O", 11L)
                        .row("P", 11L)
                        .build());

        assertQuery(
                "SELECT * FROM test_create_partitioned_bucketed_table",
                "SELECT custkey, custkey, comment, orderstatus FROM orders");

        for (int i = 1; i <= 30; i++) {
            assertQuery(
                    format("SELECT * FROM test_create_partitioned_bucketed_table WHERE custkey = %d AND custkey2 = %d", i, i),
                    format("SELECT custkey, custkey, comment, orderstatus FROM orders WHERE custkey = %d", i));
        }
    }

    @Test
    public void testCallCreateEmptyPartition()
    {
        assertUpdate("" +
                "CREATE TABLE test_call_create_empty_partition (" +
                "  dummy_col bigint," +
                "  part varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'part' ] " +
                ")");
        getQueryRunner().execute(format("GRANT SELECT ON \"test_call_create_empty_partition$partitions\" TO ROLE %s", ACCOUNT_ADMIN));
        assertQuery("SELECT count(*) FROM \"test_call_create_empty_partition$partitions\"", "SELECT 0");

        assertUpdate("CALL system.create_empty_partition('tpch', 'test_call_create_empty_partition', ARRAY['part'], ARRAY['empty'])");
        assertQuery("SELECT count(*) FROM \"test_call_create_empty_partition$partitions\"", "SELECT 1");
    }

    @Test
    public void testOptimize()
    {
        assertUpdate("CREATE TABLE test_optimize (LIKE nation)");
        for (int i = 0; i < 10; i++) {
            assertUpdate("INSERT INTO test_optimize SELECT * FROM nation", "SELECT count(*) FROM nation");
        }
        long fileCount = (long) computeActual("SELECT count(DISTINCT \"$path\") FROM test_optimize").getOnlyValue();

        assertThatThrownBy(() -> computeActual("ALTER TABLE test_optimize EXECUTE optimize(file_size_threshold => '10kB')"))
                .hasMessage("OPTIMIZE procedure must be explicitly enabled via non_transactional_optimize_enabled session property");

        assertUpdate(
                Session.builder(getSession())
                        .setCatalogSessionProperty("objectstore", "non_transactional_optimize_enabled", "true")
                        .build(),
                "ALTER TABLE test_optimize EXECUTE optimize(file_size_threshold => '10kB')");

        long newFileCount = (long) computeActual("SELECT count(DISTINCT \"$path\") FROM test_optimize").getOnlyValue();
        assertThat(newFileCount).isLessThan(fileCount);
    }

    @Test
    public void testAnalyzePartitionedTable()
    {
        String tableName = "test_analyze_partitioned_table";
        createPartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // No column stats after running an empty analyze
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), 0);
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on 3 partitions including a null partition and a duplicate partition
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1', '7'], ARRAY['p2', '7'], ARRAY['p2', '7'], ARRAY[NULL, NULL]])", tableName), 12);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");

        // Partition [p3, 8], [e1, 9], [e2, 9] have no column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        // All partitions except empty partitions have column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1'), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2'), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7'), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7'), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '2', '3'), " +
                        "('c_double', null, 2.0, 0.5, null, '3.4', '4.4'), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8'), " +
                        "(null, null, null, null, 4.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_double', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_timestamp', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('c_varbinary', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null), " +
                        "('p_bigint', 0.0, 0.0, 1.0, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null)");

        // Drop the partitioned test table
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testUpdateNotNullColumn()
    {
        assertQueryFails(
                "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                format("Hive tables do not support NOT NULL columns"));
    }

    @Test
    public void testAnalyzeUnpartitionedTable()
    {
        String tableName = "test_analyze_unpartitioned_table";
        createUnpartitionedTableForAnalyzeTest(tableName);

        // No column stats before ANALYZE
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.375, null, null, null), " +
                        "('c_bigint', null, 8.0, 0.375, null, '0', '7'), " +
                        "('c_double', null, 10.0, 0.375, null, '1.2', '7.7'), " +
                        "('c_timestamp', null, 10.0, 0.375, null, null, null), " +
                        "('c_varchar', 40.0, 10.0, 0.375, null, null, null), " +
                        "('c_varbinary', 20.0, null, 0.375, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8'), " +
                        "(null, null, null, null, 16.0, null, null)");

        // Drop the unpartitioned test table
        assertUpdate("DROP TABLE " + tableName);
    }

    protected void createPartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, true);
    }

    protected void createUnpartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, false);
    }

    private void createTableForAnalyzeTest(String tableName, boolean partitioned)
    {
        Session defaultSession = getSession();

        // Disable column statistics collection when creating the table
        Session disableColumnStatsSession = Session.builder(defaultSession)
                .setCatalogSessionProperty(defaultSession.getCatalog().get(), "collect_column_statistics_on_write", "false")
                .build();

        assertUpdate(
                disableColumnStatsSession,
                "" +
                        "CREATE TABLE " +
                        tableName +
                        (partitioned ? " WITH (partitioned_by = ARRAY['p_varchar', 'p_bigint'])\n" : " ") +
                        "AS " +
                        "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar, p_bigint " +
                        "FROM ( " +
                        "  VALUES " +
                        // p_varchar = 'p1', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00:00.000', 'abc1', X'bcd1', 'p1', BIGINT '7'), " +
                        "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00:00.000', 'abc2', X'bcd2', 'p1', BIGINT '7'), " +
                        // p_varchar = 'p2', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00:00.000', 'cba1', X'dcb1', 'p2', BIGINT '7'), " +
                        "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00:00.000', 'cba2', X'dcb2', 'p2', BIGINT '7'), " +
                        // p_varchar = 'p3', p_bigint = BIGINT '8'
                        "    (null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (true, BIGINT '3', DOUBLE '4.4', TIMESTAMP '2012-10-10 01:00:00.000', 'bca1', X'cdb1', 'p3', BIGINT '8'), " +
                        "    (false, BIGINT '2', DOUBLE '3.4', TIMESTAMP '2012-10-10 00:00:00.000', 'bca2', X'cdb2', 'p3', BIGINT '8'), " +
                        // p_varchar = NULL, p_bigint = NULL
                        "    (false, BIGINT '7', DOUBLE '7.7', TIMESTAMP '1977-07-07 07:07:00.000', 'efa1', X'efa1', NULL, NULL), " +
                        "    (false, BIGINT '6', DOUBLE '6.7', TIMESTAMP '1977-07-07 07:06:00.000', 'efa2', X'efa2', NULL, NULL), " +
                        "    (false, BIGINT '5', DOUBLE '5.7', TIMESTAMP '1977-07-07 07:05:00.000', 'efa3', X'efa3', NULL, NULL), " +
                        "    (false, BIGINT '4', DOUBLE '4.7', TIMESTAMP '1977-07-07 07:04:00.000', 'efa4', X'efa4', NULL, NULL) " +
                        ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, p_varchar, p_bigint)", 16);

        if (partitioned) {
            // Create empty partitions
            assertUpdate(disableColumnStatsSession, format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e1", "9"));
            assertUpdate(disableColumnStatsSession, format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e2", "9"));
        }
    }
}
