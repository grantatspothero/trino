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

import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

public class TestObjectStoreHudiConnectorTest
        extends BaseObjectStoreConnectorTest
{
    public TestObjectStoreHudiConnectorTest()
    {
        super(HUDI);
    }

    @Override
    protected void initializeTpchTables(DistributedQueryRunner queryRunner, TestingGalaxyMetastore metastore)
    {
        ObjectStoreQueryRunner.initializeTpchTablesHudi(queryRunner, REQUIRED_TPCH_TABLES, metastore);
    }

    @Override
    @SuppressWarnings("DuplicateBranchesInSwitch")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        boolean connectorHasBehavior = new GetHudiConnectorTestBehavior().hasBehavior(connectorBehavior);

        switch (connectorBehavior) {
            case SUPPORTS_DROP_SCHEMA_CASCADE:
                return true;

            // ObjectStore adds support for schemas using Hive
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_RENAME_SCHEMA:
                // when this fails remove the `case` for given flag
                verify(!connectorHasBehavior, "Unexpected support for: %s", connectorBehavior);
                return true;

            // ObjectStore adds support for materialized views using Iceberg
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW_GRACE_PERIOD:
            case SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW:
            case SUPPORTS_MATERIALIZED_VIEW_FRESHNESS_FROM_BASE_TABLES:
            case SUPPORTS_RENAME_MATERIALIZED_VIEW:
//            case SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS: -- not supported by Iceberg
            case SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN:
                // when this fails remove the `case` for given flag
                verify(!connectorHasBehavior, "Unexpected support for: %s", connectorBehavior);
                return true;

            // ObjectStore adds support for views using Hive
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_COMMENT_ON_VIEW:
            case SUPPORTS_COMMENT_ON_VIEW_COLUMN:
                // when this fails remove the `case` for given flag
                verify(!connectorHasBehavior, "Unexpected support for: %s", connectorBehavior);
                return true;

            case SUPPORTS_CREATE_FUNCTION:
                return false;

            default:
                // By default, declare all behaviors/features supported by Hudi connector
                return connectorHasBehavior;
        }
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue()).matches("" +
                "\\QCREATE TABLE objectstore.tpch.orders (\n" +
                "   _hoodie_commit_time varchar,\n" +
                "   _hoodie_commit_seqno varchar,\n" +
                "   _hoodie_record_key varchar,\n" +
                "   _hoodie_partition_path varchar,\n" +
                "   _hoodie_file_name varchar,\n" +
                "   orderkey bigint,\n" +
                "   custkey bigint,\n" +
                "   orderstatus varchar(1),\n" +
                "   totalprice double,\n" +
                "   orderdate date,\n" +
                "   orderpriority varchar(15),\n" +
                "   clerk varchar(15),\n" +
                "   shippriority integer,\n" +
                "   comment varchar(79),\n" +
                "   _uuid varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   location = '\\E.*\\Q/data/orders',\n" +
                "   type = 'HUDI'\n" +
                ")\\E");
    }

    @Override
    public void testDescribeTable()
    {
        // The Hudi connector includes additional metadata columns which are not hidden. This means the nation table does not have the expected schema.
        // This is a copy of the test from the base class with any `SELECT *`s replaced with explicit column lists.
        assertColumns("DESCRIBE orders");
    }

    @Override
    public void testShowColumns()
    {
        // The Hudi connector includes additional metadata columns which are not hidden. This means the nation table does not have the expected schema.
        // This is a copy of the test from the base class with any `SELECT *`s replaced with explicit column lists.
        assertColumns("SHOW COLUMNS FROM orders");
    }

    protected void assertColumns(@Language("SQL") String sql)
    {
        MaterializedResult actual = computeActual(sql);

        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("_hoodie_commit_time", "varchar", "", "")
                .row("_hoodie_commit_seqno", "varchar", "", "")
                .row("_hoodie_record_key", "varchar", "", "")
                .row("_hoodie_partition_path", "varchar", "", "")
                .row("_hoodie_file_name", "varchar", "", "")
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar(79)", "", "")
                .row("_uuid", "varchar", "", "")
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageMatching("""
                        ^
                        \\QMultiple Failures (1 failure)
                        -- failure 1 --
                        [Rows for query [SHOW COLUMNS FROM test_materialized_view_\\E\\w+\\Q projected with [Column]]]\s
                        Expecting actual:
                          (_hoodie_commit_time), (_hoodie_commit_seqno), (_hoodie_record_key), (_hoodie_partition_path), (_hoodie_file_name), (nationkey), (name), (regionkey), (comment), (_uuid)
                        to contain exactly in any order:
                          [(nationkey), (name), (regionkey), (comment)]
                        but the following elements were unexpected:
                          (_hoodie_commit_time), (_hoodie_commit_seqno), (_hoodie_record_key), (_hoodie_partition_path), (_hoodie_file_name), (_uuid)
                        at QueryAssertions$QueryAssert.\\E.*""");
    }

    @Override
    public void testCreateTable()
    {
        assertQueryFails("CREATE TABLE %s%s (x bigint)".formatted("test_create_", randomNameSuffix()),
                "Table creation is not supported for Hudi");
    }

    @Override
    public void testCreateTableAsSelect()
    {
        assertQueryFails("CREATE TABLE %s%s AS SELECT name, regionkey FROM nation".formatted("test_create_", randomNameSuffix()),
                "Table creation is not supported for Hudi");
    }

    @Override
    public void testCreateTableAsSelectNegativeDate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        super.testCreateTableAsSelectNegativeDate();
    }

    @Override
    public void testCommentTable()
    {
        assertQueryFails("COMMENT ON TABLE nation IS 'new comment'", "Setting comments for Hudi tables is not supported");
    }

    @Override
    public void testCommentColumn()
    {
        assertQueryFails("COMMENT ON COLUMN nation.nationkey IS 'new comment'", "Setting column comments in Hudi tables is not supported");
    }

    @Override
    public void testAddColumn()
    {
        assertQueryFails("ALTER TABLE nation ADD COLUMN test_add_column bigint", "Adding columns to Hudi tables is not supported");
    }

    @Override
    public void testRenameColumn()
    {
        assertQueryFails("ALTER TABLE nation RENAME COLUMN nationkey TO test_rename_column", "Renaming columns in Hudi tables is not supported");
    }

    @Override
    public void testDropColumn()
    {
        assertQueryFails("ALTER TABLE nation DROP COLUMN nationkey", "Dropping columns from Hudi tables is not supported");
    }

    @Override
    public void testInsert()
    {
        assertQueryFails("INSERT INTO nation (nationkey) VALUES (42)", "Writes are not supported for Hudi tables");
    }

    @Override
    public void testInsertNegativeDate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_INSERT));
        super.testInsertNegativeDate();
    }

    @Override
    public void testUpdate()
    {
        assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", "Writes are not supported for Hudi tables");
    }

    @Override
    public void testUpdateRowType()
    {
        // TODO: when updates are generally supported, replace this query with something that exercises ROW type, e.g. by removing the override.
        assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", "Writes are not supported for Hudi tables");
    }

    @Override
    public void testUpdateWithPredicates()
    {
        skipTestUnless(hasBehavior(SUPPORTS_UPDATE));
        super.testUpdateWithPredicates();
    }

    @Override
    public void testUpdateAllValues()
    {
        skipTestUnless(hasBehavior(SUPPORTS_UPDATE));
        super.testUpdateAllValues();
    }

    @Override
    public void testMaterializedViewGracePeriod()
    {
        assertFalse(hasBehavior(SUPPORTS_CREATE_TABLE)); // remove override when true
        assertThatThrownBy(super::testMaterializedViewGracePeriod)
                .hasMessage("Table creation is not supported for Hudi");
        throw new SkipException("test not implemented");
    }

    @Override
    public void testMaterializedViewBaseTableGone(boolean initialized)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        super.testMaterializedViewBaseTableGone(initialized);
    }

    @Override
    public void testCompatibleTypeChangeForView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        super.testCompatibleTypeChangeForView();
    }

    @Override
    public void testCompatibleTypeChangeForView2()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        super.testCompatibleTypeChangeForView2();
    }

    @Override
    public void testDropNonEmptySchemaWithTable()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        super.testDropNonEmptySchemaWithTable();
    }

    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
            throws Exception
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        super.testReadMetadataWithRelationsConcurrentModifications();
    }

    @Override
    public void testHiveSpecificTableProperty()
    {
        assertThatThrownBy(super::testHiveSpecificTableProperty)
                .hasMessage("Table creation is not supported for Hudi");
    }

    @Override
    public void testHiveSpecificColumnProperty()
    {
        assertThatThrownBy(super::testHiveSpecificColumnProperty)
                .hasMessage("Table creation is not supported for Hudi");
    }

    @Override
    public void testIcebergSpecificTableProperty()
    {
        assertThatThrownBy(super::testIcebergSpecificTableProperty)
                .hasMessage("Table creation is not supported for Hudi");
    }

    @Override
    public void testDeltaSpecificTableProperty()
    {
        assertThatThrownBy(super::testDeltaSpecificTableProperty)
                .hasMessage("Table creation is not supported for Hudi");
    }

    @Override
    public void testRegisterTableProcedure()
    {
        // Override because Hudi connector doesn't support creating a table
        // Verify failure by registering an existing table content with different name
        String tableLocation = getTableLocation("region");
        assertQueryFails(
                "CALL system.register_table (CURRENT_SCHEMA, '" + ("test_register_table_" + randomNameSuffix()) + "', '" + tableLocation + "')",
                "Registering Hudi tables is unsupported");
    }

    @Override
    public void testRegisterTableProcedureIcebergSpecificArgument()
    {
        // Override because Hudi connector doesn't support creating a table
        assertThatThrownBy(super::testRegisterTableProcedureIcebergSpecificArgument)
                .hasMessageContaining("Table creation is not supported for Hudi");
    }

    @Override
    public void testUnregisterTableProcedure()
    {
        // Override because Hudi connector doesn't support creating a table
        // Use an existing table to verify the failure
        assertQueryFails("CALL system.unregister_table(CURRENT_SCHEMA, 'region')", "Unsupported table type");
    }

    @Override
    public void testUnregisterTableAccessControl()
    {
        // Override because Hudi connector doesn't support creating a table
        // Use an existing table to verify the failure
        assertAccessDenied(
                "CALL system.unregister_table(CURRENT_SCHEMA, 'region')",
                "Cannot drop table .*",
                privilege("region", DROP_TABLE));
    }

    @Override
    public void testMigrateToIcebergTable()
    {
        // Override because Hudi connector doesn't support creating a table
        assertQueryFails(
                "ALTER TABLE region SET PROPERTIES type = 'ICEBERG'",
                "Changing table type from 'HUDI' to 'ICEBERG' is not supported");
    }

    @Override
    public void testMigrateToDeltaTable()
    {
        // Override because Hudi connector doesn't support creating a table
        assertQueryFails(
                "ALTER TABLE region SET PROPERTIES type = 'DELTA'",
                "Changing table type from 'HUDI' to 'DELTA' is not supported");
    }

    @Override
    public void testMigrateToHiveTable()
    {
        // Override because Hudi connector doesn't support creating a table
        assertQueryFails(
                "ALTER TABLE region SET PROPERTIES type = 'HIVE'",
                "Changing table type from 'HUDI' to 'HIVE' is not supported");
    }

    @Override
    public void testMigrateToHudiTable()
    {
        // Override because Hudi connector doesn't support creating a table
        assertQueryFails(
                "ALTER TABLE region SET PROPERTIES type = 'HUDI'",
                "Changing table type from 'HUDI' to 'HUDI' is not supported");
    }

    @Override
    public void testSelectAll()
    {
        throw new SkipException("Hudi returns extra columns");
    }

    @Override
    public void testSelectInformationSchemaColumns()
    {
        throw new SkipException("Hudi returns extra columns");
    }

    @Override
    public void testDropTableCorruptStorage()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        super.testDropTableCorruptStorage();
    }
}
