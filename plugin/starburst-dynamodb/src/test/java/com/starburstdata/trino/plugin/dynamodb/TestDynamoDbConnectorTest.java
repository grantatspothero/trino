/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDynamoDbConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingDynamoDbServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingDynamoDbServer());
        return DynamoDbQueryRunner.builder(server.getEndpointUrl(), server.getSchemaDirectory())
                .setFirstColumnAsPrimaryKeyEnabled(true)
                .enablePredicatePushdown()
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return server::execute;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN:
            case SUPPORTS_DYNAMIC_FILTER_PUSHDOWN:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
                return true;
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
            case SUPPORTS_ARRAY:
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_JOIN_PUSHDOWN:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_SET_COLUMN_TYPE:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_DELETE:
            case SUPPORTS_ROW_LEVEL_DELETE:
            case SUPPORTS_CANCELLATION:
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_INSERT:
            case SUPPORTS_TRUNCATE:
            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_MERGE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_NATIVE_QUERY:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "orderkey")
                .row("custkey", "bigint", "", "custkey")
                .row("orderstatus", "varchar(1)", "", "orderstatus")
                .row("totalprice", "double", "", "totalprice")
                .row("orderdate", "date", "", "orderdate")
                .row("orderpriority", "varchar(15)", "", "orderpriority")
                .row("clerk", "varchar(15)", "", "clerk")
                .row("shippriority", "integer", "", "shippriority")
                .row("comment", "varchar(79)", "", "comment")
                .build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("""
                        CREATE TABLE \\w+\\.\\w+\\.orders \\Q(
                           orderkey bigint NOT NULL COMMENT 'orderkey',
                           custkey bigint COMMENT 'custkey',
                           orderstatus varchar(1) COMMENT 'orderstatus',
                           totalprice double COMMENT 'totalprice',
                           orderdate date COMMENT 'orderdate',
                           orderpriority varchar(15) COMMENT 'orderpriority',
                           clerk varchar(15) COMMENT 'clerk',
                           shippriority integer COMMENT 'shippriority',
                           comment varchar(79) COMMENT 'comment'
                        )""");
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
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        if (ImmutableSet.of("atrailingspace ", " aleadingspace", "a.dot", "a,comma", "a\"quote", "a\\backslash`").contains(columnName)) {
            return Optional.empty();
        }
        return Optional.of(columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.startsWith("decimal") || typeName.equals("char(3)") || typeName.startsWith("time")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    @Test
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "orderkey")
                .row("custkey", "bigint", "", "custkey")
                .row("orderstatus", "varchar(1)", "", "orderstatus")
                .row("totalprice", "double", "", "totalprice")
                .row("orderdate", "date", "", "orderdate")
                .row("orderpriority", "varchar(15)", "", "orderpriority")
                .row("clerk", "varchar(15)", "", "clerk")
                .row("shippriority", "integer", "", "shippriority")
                .row("comment", "varchar(79)", "", "comment")
                .build();

        assertThat(expectedParametrizedVarchar)
                .withFailMessage(format("%s does not match %s", actual, expectedParametrizedVarchar))
                .containsExactlyElementsOf(actual);
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining(
                "SQL compilation error: Non-nullable column 'C_VARCHAR' cannot be added to non-empty table " +
                        "'TEST_ADD_NOTNULL_.*' unless it has a non-null default value\\.");
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharTrailingSpace)
                .hasMessageMatching("Failed to execute statement: CREATE TABLE amazondynamodb.* \\(x char\\(10\\)\\)");
    }
}