/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse;

import io.trino.Session;
import io.trino.plugin.sqlserver.BaseSqlServerConnectorTest;
import io.trino.plugin.sqlserver.DataCompression;
import io.trino.testing.DataProviders;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testng.services.ManageTestResources;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugins.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.NON_TRANSACTIONAL_INSERT;
import static io.trino.plugin.sqlserver.SqlServerSessionProperties.BULK_COPY_FOR_WRITE;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.trueFalse;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

public class TestSynapseConnectorTest
        extends BaseSqlServerConnectorTest
{
    public static final String CATALOG = "sqlserver";
    @ManageTestResources.Suppress(because = "Mock to remote server")
    private SynapseServer synapseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                Map.of(),
                synapseServer,
                CATALOG,
                // Synapse tests are slow. Cache metadata to speed them up. Synapse without caching is exercised by TestSynapseConnectorSmokeTest.
                Map.of("metadata.cache-ttl", "60m"),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            // Overriden because Synapse disables connector expression pushdown due to correctness issues with varchar pushdown because of default case-insensitive collation
            case SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return synapseServer::execute;
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        throw new SkipException("All Synapse types are mapped either by Trino or the SQL Server JDBC");
    }

    @Test
    @Override // default test execution too long due to wildcards in LIKE clause
    public void testSelectInformationSchemaColumns()
    {
        String schema = getSession().getSchema().get();
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'region'"))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment'");
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '_egion'"))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment'");
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%egio%'"))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment'");
    }

    @Test
    @Override // Needs an override because the SQL Server override is different from the base version of the test
    public void testColumnComment()
    {
        throw new SkipException("Synapse does not support column comments");
    }

    @Test
    public void testDecimalPredicatePushdown()
            throws Exception
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_decimal_pushdown", "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES (123.321, 123456789.987654321)", table.getName()));

            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal <= 124", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal <= 124", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE long_decimal <= 123456790", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal <= 123.321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE long_decimal <= 123456789.987654321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal = 123.321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE long_decimal = 123456789.987654321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override // synapse doesn't support data_compression, so reverse SQL Server's override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")");
    }

    @Test(dataProvider = "dataCompression")
    @Override
    public void testCreateWithDataCompression(DataCompression compression)
    {
        throw new SkipException("data_compression not supported in Synapse");
    }

    @Test
    @Override
    public void testShowCreateForPartitionedTablesWithDataCompression()
    {
        throw new SkipException("CREATE PARTITION FUNCTION and data_compression not supported in Synapse");
    }

    @Test
    @Override
    public void testShowCreateForIndexedAndCompressedTable()
    {
        throw new SkipException("data_compression not supported in Synapse");
    }

    @Test
    @Override
    public void testShowCreateForUniqueConstraintCompressedTable()
    {
        throw new SkipException("data_compression not supported in Synapse");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return super.filterDataMappingSmokeTestData(dataMappingTestSetup);
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testCreateTableAsSelectWriteBulkiness(boolean bulkCopyForWrite)
    {
        String table = "bulk_copy_ctas_" + randomNameSuffix();
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE, Boolean.toString(bulkCopyForWrite))
                .build();

        // loading data takes too long for this test, this test does not compare performance, just checks if the path passes, therefore LIMIT 1 is applied
        assertQuerySucceeds(session, format("CREATE TABLE %s as SELECT * FROM tpch.tiny.customer LIMIT 1", table));

        // check that there are no locks remained on the target table after bulk copy
        assertQuery("SELECT count(*) FROM " + table, "VALUES 1");
        assertUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table), 1);
        assertQuery("SELECT count(*) FROM " + table, "VALUES 2");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testSpecialCharacterColumnNameFailToRename()
    {
        String table = "special_column_name_" + randomNameSuffix();
        String specialCharacterColumnName = "\"" + "[tricky]" + "\"";
        String normalColumnName = "normal";
        Session session = Session.builder(getSession()).build();
        assertQuerySucceeds(session, format("CREATE TABLE %s (%s bigint)", table, specialCharacterColumnName));

        // check that we are not able to rename column with special character name back,
        // our test should fail after synapse will fix this issue, we will be able to add support for such cases
        this.assertQueryFails(format("ALTER TABLE %s RENAME COLUMN %s TO %s", table, specialCharacterColumnName, normalColumnName), "\\QEither the parameter @objname is ambiguous or the claimed @objtype (COLUMN) is wrong.\\E");
    }

    @Test(dataProvider = "doubleTrueFalse")
    public void testInsertWriteBulkiness(boolean nonTransactionalInsert, boolean bulkCopyForWrite)
    {
        String table = "bulk_copy_insert_" + randomNameSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s as SELECT * FROM tpch.tiny.customer WHERE 0 = 1", table));
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, NON_TRANSACTIONAL_INSERT, Boolean.toString(nonTransactionalInsert))
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE, Boolean.toString(bulkCopyForWrite))
                .build();

        // loading data takes too long for this test, this test does not compare performance, just checks if the path passes, therefore LIMIT 1 is applied
        assertQuerySucceeds(session, format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table));

        // check that there are no locks remained on the target table after bulk copy
        assertQuery("SELECT count(*) FROM " + table, "VALUES 1");
        assertUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table), 1);
        assertQuery("SELECT count(*) FROM " + table, "VALUES 2");

        assertUpdate("DROP TABLE " + table);
    }

    @Override
    public void testDelete()
    {
        // TODO: Remove override once superclass uses smaller tables to test (because INSERTs to Synapse are slow)
        skipTestUnless(hasBehavior(SUPPORTS_DELETE) && hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName = "test_delete_" + randomNameSuffix();

        // delete successive parts of the table
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", "SELECT count(*) FROM nation");

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey <= 5", "SELECT count(*) FROM nation WHERE nationkey <= 5");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE nationkey > 5");

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey <= 10", "SELECT count(*) FROM nation WHERE nationkey > 5 AND nationkey <= 10");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE nationkey > 10");

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey <= 20", "SELECT count(*) FROM nation WHERE nationkey > 10 AND nationkey <= 20");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE nationkey > 20");

        assertUpdate("DROP TABLE " + tableName);

        // delete without matching any rows
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", "SELECT count(*) FROM nation");
        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey < 0", 0);
        assertUpdate("DROP TABLE " + tableName);

        // delete with a predicate that optimizes to false
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", "SELECT count(*) FROM nation");
        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey > 5 AND nationkey < 4", 0);
        assertUpdate("DROP TABLE " + tableName);

        // test EXPLAIN ANALYZE with CTAS
        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT nationkey FROM nation");
        assertQuery("SELECT * from " + tableName, "SELECT nationkey FROM nation");
        // check that INSERT works also
        assertExplainAnalyze("EXPLAIN ANALYZE INSERT INTO " + tableName + " SELECT regionkey FROM nation");
        assertQuery("SELECT * from " + tableName, "SELECT nationkey FROM nation UNION ALL SELECT regionkey FROM nation");
        // check DELETE works with EXPLAIN ANALYZE
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE TRUE");
        assertQuery("SELECT COUNT(*) from " + tableName, "SELECT 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: This connector does not support modifying table rows");
    }

    @Override
    @Test
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        // TODO Fix concurrent metadata modification test https://starburstdata.atlassian.net/browse/SEP-8789
        throw new SkipException("Test fails with a timeout sometimes and is flaky");
    }

    @Override
    public void testInsertRowConcurrently()
    {
        throw new SkipException("Synapse INSERTs are slow and the futures sometimes timeout in the test. TODO https://starburstdata.atlassian.net/browse/SEP-9214");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("(?s)Cannot insert the value NULL into column '%s'.*", columnName);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Parse Error: Identifier '.*' exceeded the maximum length of 128.");
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e)
                .hasMessageMatching("(Parse Error: Identifier '.*' exceeded the maximum length of 128.|Table name must be shorter than or equal to '128' characters but got '129')");
    }

    @DataProvider
    public static Object[][] doubleTrueFalse()
    {
        return cartesianProduct(trueFalse(), trueFalse());
    }

    @Override // Override because the JDBC prepares the query, but does not provide ResultSetMetadata
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        assertFalse(getQueryRunner().tableExists(getSession(), "non_existent_table"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO non_existent_table VALUES (1)'))"))
                .hasMessageContaining("Query not supported: ResultSetMetaData not available for query: INSERT INTO non_existent_table VALUES (1)");
    }
}
