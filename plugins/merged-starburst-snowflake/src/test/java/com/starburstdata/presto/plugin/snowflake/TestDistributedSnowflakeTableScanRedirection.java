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

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.jdbc.redirection.AbstractTableScanRedirectionTest;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;

public class TestDistributedSnowflakeTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    protected final SnowflakeServer server = new SnowflakeServer();

    @Override
    protected QueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return distributedBuilder()
                .withServer(server)
                .withAdditionalProperties(ImmutableMap.<String, String>builder()
                        .putAll(impersonationDisabled())
                        .putAll(getRedirectionProperties("snowflake", TEST_SCHEMA))
                        .build())
                .build();
    }
}