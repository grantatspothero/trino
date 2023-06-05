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
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

/**
 * Tests ObjectStore connector with Iceberg backend.
 *
 * @see TestObjectStoreIcebergFeaturesConnectorTest
 */
public class TestObjectStoreIcebergConnectorTest
        extends BaseObjectStoreConnectorTest
{
    private long v1SnapshotId;
    private long v1EpochMillis;
    private long v2SnapshotId;
    private long v2EpochMillis;
    private long incorrectSnapshotId;

    public TestObjectStoreIcebergConnectorTest()
    {
        super(ICEBERG);
    }

    @Override
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        boolean connectorHasBehavior = new GetIcebergConnectorTestBehavior().hasBehavior(connectorBehavior);

        switch (connectorBehavior) {
            case SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS:
                // TODO when this changes, remove this flag from other BaseObjectStoreConnectorTest subclasses
                verify(!connectorHasBehavior, "Unexpected support for: %s", connectorBehavior);
                return false;

            default:
                // By default, declare all behaviors/features supported by Iceberg connector
                return connectorHasBehavior;
        }
    }

    @BeforeClass
    public void setUp()
            throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_iceberg_read_versioned_table(a_string varchar, an_integer integer)");
        assertQuerySucceeds("INSERT INTO test_iceberg_read_versioned_table VALUES ('a', 1)");
        getQueryRunner().execute(format("GRANT SELECT ON \"test_iceberg_read_versioned_table$snapshots\" TO ROLE %s", ACCOUNT_ADMIN));
        v1SnapshotId = getLatestSnapshotId("test_iceberg_read_versioned_table");
        v1EpochMillis = getCommittedAtInEpochMilliSeconds("test_iceberg_read_versioned_table", v1SnapshotId);
        TimeUnit.MILLISECONDS.sleep(1);
        assertQuerySucceeds("INSERT INTO test_iceberg_read_versioned_table VALUES ('b', 2)");
        v2SnapshotId = getLatestSnapshotId("test_iceberg_read_versioned_table");
        v2EpochMillis = getCommittedAtInEpochMilliSeconds("test_iceberg_read_versioned_table", v2SnapshotId);
        incorrectSnapshotId = v2SnapshotId + 1;
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageContaining("Failed to commit Iceberg update to table");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageContaining("Cannot update Iceberg table: supplied previous location does not match current location");
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "bigint -> integer":
            case "decimal(5,3) -> decimal(5,2)":
            case "varchar -> char(20)":
            case "time(6) -> time(3)":
            case "timestamp(6) -> timestamp(3)":
            case "array(integer) -> array(bigint)":
                // Iceberg allows updating column types if the update is safe. Safe updates are:
                // - int to bigint
                // - float to double
                // - decimal(P,S) to decimal(P2,S) when P2 > P (scale cannot change)
                // https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table--alter-column
                return Optional.of(setup.asUnsupported());

            case "varchar(100) -> varchar(50)":
                // Iceberg connector ignores the varchar length
                return Optional.empty();
        }
        return Optional.of(setup);
    }

    @Override
    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*(Cannot change column type|not supported for Iceberg|Not a primitive type|Cannot change type ).*");
    }

    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessage("Type not supported for Iceberg: char(3)");
    }

    @Override
    protected void verifyVersionedQueryFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageMatching("Version pointer type is not supported: .*|" +
                        "Unsupported type for temporal table version: .*|" +
                        "Unsupported type for table version: .*|" +
                        "No version history table tpch.nation at or before .*|" +
                        "Iceberg snapshot ID does not exists: .*");
    }

    @Override
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue()).matches("" +
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
                "   format = 'ORC',\n" +
                "   format_version = 2,\n" +
                "   location = 's3://test-bucket/tpch/orders-\\E.*\\Q',\n" +
                "   type = 'ICEBERG'\n" +
                ")\\E");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("tinyint")
                || typeName.equals("smallint")
                || typeName.startsWith("char(")) {
            // These types are not supported by Iceberg
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        // According to Iceberg specification all time and timestamp values are stored with microsecond precision.
        if (typeName.equals("time")) {
            return Optional.of(new DataMappingTestSetup("time(6)", "TIME '15:03:00'", "TIME '23:59:59.999999'"));
        }

        if (typeName.equals("timestamp")) {
            return Optional.of(new DataMappingTestSetup("timestamp(6)", "TIMESTAMP '2020-02-12 15:03:00'", "TIMESTAMP '2199-12-31 23:59:59.999999'"));
        }

        if (typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(new DataMappingTestSetup("timestamp(6) with time zone", "TIMESTAMP '2020-02-12 15:03:00 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999999 +12:00'"));
        }

        return Optional.of(dataMappingTestSetup);
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
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "NULL value not allowed for NOT NULL column: " + columnName;
    }

    @Override
    public void testHiveSpecificTableProperty()
    {
        assertThatThrownBy(super::testHiveSpecificTableProperty)
                .hasMessage("Table property 'auto_purge' not supported for Iceberg tables");
    }

    @Override
    public void testDeltaSpecificTableProperty()
    {
        assertThatThrownBy(super::testDeltaSpecificTableProperty)
                .hasMessage("Table property 'checkpoint_interval' not supported for Iceberg tables");
    }

    @Override
    protected Double basicTableStatisticsExpectedNdv(int actualNdv)
    {
        return (double) actualNdv;
    }

    @Test
    public void testCallRollbackToSnapshot()
    {
        assertUpdate("CREATE TABLE test_rollback AS SELECT 123 x", 1);
        getQueryRunner().execute(format("GRANT SELECT ON \"test_rollback$snapshots\" TO ROLE %s", ACCOUNT_ADMIN));
        long snapshotId = (long) computeActual("SELECT snapshot_id FROM \"test_rollback$snapshots\"").getOnlyValue();
        assertUpdate("INSERT INTO test_rollback VALUES (456)", 1);
        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", snapshotId));
        assertQuery("SELECT * FROM test_rollback", "VALUES 123");
    }

    @Test
    public void testOptimize()
    {
        assertUpdate("CREATE TABLE test_optimize (LIKE nation) WITH (format_version = 1)");
        getQueryRunner().execute(format("GRANT SELECT ON \"test_optimize$files\" TO ROLE %s", ACCOUNT_ADMIN));
        for (int i = 0; i < 10; i++) {
            assertUpdate("INSERT INTO test_optimize SELECT * FROM nation", "SELECT count(*) FROM nation");
        }
        long fileCount = (long) computeActual("SELECT count(DISTINCT file_path) FROM \"test_optimize$files\"").getOnlyValue();

        assertUpdate("ALTER TABLE test_optimize EXECUTE OPTIMIZE");

        long newFileCount = (long) computeActual("SELECT count(DISTINCT file_path) FROM \"test_optimize$files\"").getOnlyValue();
        assertThat(newFileCount).isLessThan(fileCount);

        assertQuerySucceeds("ALTER TABLE test_optimize EXECUTE expire_snapshots");
        assertQuerySucceeds("ALTER TABLE test_optimize EXECUTE remove_orphan_files");
    }

    @Test
    public void testAnalyze()
    {
        assertUpdate("CREATE TABLE test_analyze (x BIGINT)");
        assertUpdate("ANALYZE test_analyze");
        assertQuery(
                "SHOW STATS FOR test_analyze",
                """
                        VALUES
                          ('x', 0, 0, 1, null, null, null),
                          (null, null, null, null, 0, null, null)""");

        assertUpdate("DROP TABLE test_analyze");
    }

    // Versioned table tests from TestIcebergReadVersionedTable

    @Test
    public void testSelectTableWithEndSnapshotId()
    {
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR VERSION AS OF " + v1SnapshotId, "VALUES ('a', 1)");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR VERSION AS OF " + v2SnapshotId, "VALUES ('a', 1), ('b', 2)");
        assertQueryFails("SELECT * FROM test_iceberg_read_versioned_table FOR VERSION AS OF " + incorrectSnapshotId, "Iceberg snapshot ID does not exists: " + incorrectSnapshotId);
    }

    @Test
    public void testSelectTableWithEndShortTimestampWithTimezone()
    {
        assertQueryFails("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00.001000000 Z'",
                "\\QNo version history table tpch.\"test_iceberg_read_versioned_table\" at or before 1970-01-01T00:00:00.001Z");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9), "VALUES ('a', 1)");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v2EpochMillis, 9), "VALUES ('a', 1), ('b', 2)");
    }

    @Test
    public void testSelectTableWithEndLongTimestampWithTimezone()
    {
        assertQueryFails("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF TIMESTAMP '1970-01-01 00:00:00.001000000 Z'",
                "\\QNo version history table tpch.\"test_iceberg_read_versioned_table\" at or before 1970-01-01T00:00:00.001Z");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9), "VALUES ('a', 1)");
        assertQuery("SELECT * FROM test_iceberg_read_versioned_table FOR TIMESTAMP AS OF " + timestampLiteral(v2EpochMillis, 9), "VALUES ('a', 1), ('b', 2)");
    }

    @Test
    public void testEndVersionInTableNameAndForClauseShouldFail()
    {
        assertQueryFails("SELECT * FROM \"test_iceberg_read_versioned_table@" + v1SnapshotId + "\" FOR VERSION AS OF " + v1SnapshotId,
                "Invalid Iceberg table name: test_iceberg_read_versioned_table@%d".formatted(v1SnapshotId));

        assertQueryFails("SELECT * FROM \"test_iceberg_read_versioned_table@" + v1SnapshotId + "\" FOR TIMESTAMP AS OF " + timestampLiteral(v1EpochMillis, 9),
                "Invalid Iceberg table name: test_iceberg_read_versioned_table@%d".formatted(v1SnapshotId));
    }

    @Test
    public void testSortedTable()
    {
        Session withSmallRowGroups = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "orc_writer_max_stripe_rows", "7")
                .build();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_table",
                "(id INTEGER, name VARCHAR) WITH (sorted_by = ARRAY['name'], format = 'ORC')")) {
            assertUpdate(format("GRANT SELECT ON \"%s$files\" TO ROLE %s", table.getName(), ACCOUNT_ADMIN));
            String values = " VALUES" +
                    "(5, 'name5')," +
                    "(10, 'name10')," +
                    "(4, 'name4')," +
                    "(1, 'name1')," +
                    "(6, 'name6')," +
                    "(9, 'name9')," +
                    "(8, 'name8')," +
                    "(3, 'name3')," +
                    "(7, 'name7')," +
                    "(2, 'name2')";
            assertUpdate(withSmallRowGroups, "INSERT INTO " + table.getName() + values, 10);
            assertQuery("SELECT * FROM " + table.getName(), values.trim());
            TrinoFileSystemFactory fileSystemFactory = getTrinoFileSystemFactory();
            for (Object filePath : computeActual("SELECT file_path from \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertTrue(checkOrcFileSorting(fileSystemFactory, Location.of((String) filePath), "name"));
            }
        }
    }

    @Override
    public void testAddAndDropColumnName(String columnName)
    {
        if (columnName.equals("a.dot")) {
            assertThatThrownBy(() -> super.testAddAndDropColumnName(columnName))
                    .hasMessageContaining("Cannot add column with ambiguous name");
            return;
        }
        super.testAddAndDropColumnName(columnName);
    }

    @Override
    public void testDropRowFieldWhenDuplicates()
    {
        // Override because Iceberg doesn't allow duplicated field names in a row type
        assertThatThrownBy(super::testDropRowFieldWhenDuplicates)
                .hasMessage("Invalid schema: multiple fields for name col.a: 2 and 3");
    }

    @Override
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        // TODO https://github.com/trinodb/trino/issues/16273 The connector can't read row types having ambiguous field names in ORC files. e.g. row(X int, x int)
        assertThatThrownBy(super::testDropAmbiguousRowFieldCaseSensitivity)
                .hasMessageContaining("Error opening Iceberg split")
                .hasStackTraceContaining("Multiple entries with same key");
    }

    @Test
    public void testSystemTables()
    {
        // TODO https://github.com/trinodb/trino/issues/12920
        assertQueryFails("SELECT * FROM \"test_iceberg_read_versioned_table$partitions\" FOR VERSION AS OF " + v1SnapshotId,
                "This connector does not support versioned tables");
    }

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeActual(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1", tableName))
                .getOnlyValue();
    }

    private long getCommittedAtInEpochMilliSeconds(String tableName, long snapshotId)
    {
        return ((ZonedDateTime) computeActual(format("SELECT committed_at FROM \"%s$snapshots\" WHERE snapshot_id=%s LIMIT 1", tableName, snapshotId)).getOnlyValue())
                .toInstant().toEpochMilli();
    }

    private static String timestampLiteral(long epochMilliSeconds, int precision)
    {
        return DateTimeFormatter.ofPattern("'TIMESTAMP '''uuuu-MM-dd HH:mm:ss." + "S".repeat(precision) + " VV''")
                .format(Instant.ofEpochMilli(epochMilliSeconds).atZone(UTC));
    }
}
