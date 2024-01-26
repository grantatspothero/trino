/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributedSnowflakeConnectorTest
        extends BaseSnowflakeConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withTpchTables(ImmutableList.<TpchTable<?>>builder()
                        .addAll(REQUIRED_TPCH_TABLES)
                        .add(LINE_ITEM)
                        .build())
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                // TOPN is retained due to parallelism
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    public void testSimpleSelect()
    {
        assertQuery("SELECT regionkey, name FROM region ORDER BY regionkey", "VALUES (0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA'), (3, 'EUROPE'), (4, 'MIDDLE EAST')");
    }

    @Test
    @Override
    public void testSnowflakeTimestampWithPrecision()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_timestamp_with_precision",
                "(" +
                        "timestamp0 timestamp(0)," +
                        "timestamp1 timestamp(1)," +
                        "timestamp2 timestamp(2)," +
                        "timestamp3 timestamp(3)," +
                        "timestamp4 timestamp(4)," +
                        "timestamp5 timestamp(5)," +
                        "timestamp6 timestamp(6)," +
                        "timestamp7 timestamp(7)," +
                        "timestamp8 timestamp(8)," +
                        "timestamp9 timestamp(9))",
                ImmutableList.of("" +
                        "TIMESTAMP '1901-02-03 04:05:06'," +
                        "TIMESTAMP '1901-02-03 04:05:06.1'," +
                        "TIMESTAMP '1901-02-03 04:05:06.12'," +
                        "TIMESTAMP '1901-02-03 04:05:06.123'," +
                        "TIMESTAMP '1901-02-03 04:05:06.1234'," +
                        "TIMESTAMP '1901-02-03 04:05:06.12345'," +
                        "TIMESTAMP '1901-02-03 04:05:06.123456'," +
                        "TIMESTAMP '1901-02-03 04:05:06.1234567'," +
                        "TIMESTAMP '1901-02-03 04:05:06.12345678'," +
                        "TIMESTAMP '1901-02-03 04:05:06.123456789'"))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + testTable.getName()).getOnlyValue())
                    .matches("CREATE TABLE \\w+\\.\\w+\\.\\w+ \\Q(\n" +
                            "   timestamp0 timestamp(3),\n" +
                            "   timestamp1 timestamp(3),\n" +
                            "   timestamp2 timestamp(3),\n" +
                            "   timestamp3 timestamp(3),\n" +
                            "   timestamp4 timestamp(3),\n" +
                            "   timestamp5 timestamp(3),\n" +
                            "   timestamp6 timestamp(3),\n" +
                            "   timestamp7 timestamp(3),\n" +
                            "   timestamp8 timestamp(3),\n" +
                            "   timestamp9 timestamp(3)\n" +
                            ")");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES (" +
                            "TIMESTAMP '1901-02-03 04:05:06.000'," +
                            "TIMESTAMP '1901-02-03 04:05:06.100'," +
                            "TIMESTAMP '1901-02-03 04:05:06.120'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123')");
        }
    }

    @Test
    @Override
    public void testSnowflakeTimestampRounding()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_timestamp_rounding",
                "(t timestamp(9))",
                ImmutableList.of(
                        "TIMESTAMP '1901-02-03 04:05:06.123499999'",
                        "TIMESTAMP '1901-02-03 04:05:06.123900000'",
                        "TIMESTAMP '1969-12-31 23:59:59.999999999'",
                        "TIMESTAMP '2001-02-03 04:05:06.123499999'",
                        "TIMESTAMP '2001-02-03 04:05:06.123900000'"))) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "TIMESTAMP '1901-02-03 04:05:06.123'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123'," + // Snowflake truncates on cast
                            "TIMESTAMP '1969-12-31 23:59:59.999'," + // Snowflake truncates on cast
                            "TIMESTAMP '2001-02-03 04:05:06.123'," +
                            "TIMESTAMP '2001-02-03 04:05:06.123'"); // Snowflake truncates on cast
        }
    }

    @Test
    @Override
    public void testSnowflakeTimestampWithTimeZoneWithPrecision()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_timestamptz_with_precision",
                "(" +
                        "timestamptz0 timestamp_tz(0)," +
                        "timestamptz1 timestamp_tz(1)," +
                        "timestamptz2 timestamp_tz(2)," +
                        "timestamptz3 timestamp_tz(3)," +
                        "timestamptz4 timestamp_tz(4)," +
                        "timestamptz5 timestamp_tz(5)," +
                        "timestamptz6 timestamp_tz(6)," +
                        "timestamptz7 timestamp_tz(7)," +
                        "timestamptz8 timestamp_tz(8)," +
                        "timestamptz9 timestamp_tz(9))",
                ImmutableList.of("" +
                        "'1901-02-03 04:05:06 +02:00'," +
                        "'1901-02-03 04:05:06.1 +02:00'," +
                        "'1901-02-03 04:05:06.12 +02:00'," +
                        "'1901-02-03 04:05:06.123 +02:00'," +
                        "'1901-02-03 04:05:06.1234 +02:00'," +
                        "'1901-02-03 04:05:06.12345 +02:00'," +
                        "'1901-02-03 04:05:06.123456 +02:00'," +
                        "'1901-02-03 04:05:06.1234567 +02:00'," +
                        "'1901-02-03 04:05:06.12345678 +02:00'," +
                        "'1901-02-03 04:05:06.123456789 +02:00'"))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + testTable.getName()).getOnlyValue())
                    .matches("CREATE TABLE \\w+\\.\\w+\\.\\w+ \\Q(\n" +
                            "   timestamptz0 timestamp(3) with time zone,\n" +
                            "   timestamptz1 timestamp(3) with time zone,\n" +
                            "   timestamptz2 timestamp(3) with time zone,\n" +
                            "   timestamptz3 timestamp(3) with time zone,\n" +
                            "   timestamptz4 timestamp(3) with time zone,\n" +
                            "   timestamptz5 timestamp(3) with time zone,\n" +
                            "   timestamptz6 timestamp(3) with time zone,\n" +
                            "   timestamptz7 timestamp(3) with time zone,\n" +
                            "   timestamptz8 timestamp(3) with time zone,\n" +
                            "   timestamptz9 timestamp(3) with time zone\n" +
                            ")");

            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES (" +
                            "TIMESTAMP '1901-02-03 04:05:06.000 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.100 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.120 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123 +02:00')");
        }
    }

    @Test
    @Override
    public void testSnowflakeTimestampWithTimeZoneRounding()
    {
        try (TestTable testTable = new TestTable(snowflakeExecutor::execute, getSession().getSchema().orElseThrow() + ".test_timestamptz_rounding",
                "(t timestamp_tz(9))",
                ImmutableList.of(
                        "'1901-02-03 04:05:06.123499999 +02:00'",
                        "'1901-02-03 04:05:06.123900000 +02:00'",
                        "'2001-02-03 04:05:06.123499999 +02:00'",
                        "'2001-02-03 04:05:06.123900000 +02:00'"))) {
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES " +
                            "TIMESTAMP '1901-02-03 04:05:06.123 +02:00'," +
                            "TIMESTAMP '1901-02-03 04:05:06.123 +02:00'," + // Snowflake truncates on cast
                            "TIMESTAMP '2001-02-03 04:05:06.123 +02:00'," +
                            "TIMESTAMP '2001-02-03 04:05:06.123 +02:00'"); // Snowflake truncates on cast
        }
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        if (dataMappingTestSetup.getTrinoTypeName().equals("date")) {
            // TODO (https://starburstdata.atlassian.net/browse/SEP-7956) Fix incorrect date issue in Snowflake
            if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '0001-01-01'")) {
                return Optional.empty();
            }
        }
        return super.filterDataMappingSmokeTestData(dataMappingTestSetup);
    }
}
