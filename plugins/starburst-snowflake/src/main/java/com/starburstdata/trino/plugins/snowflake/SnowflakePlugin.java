/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugins.snowflake.distributed.SnowflakeDistributedConnectorFactory;
import com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcClientModule;
import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

public class SnowflakePlugin
        implements Plugin
{
    static final String SNOWFLAKE_JDBC = "snowflake-jdbc";
    static final String SNOWFLAKE_DISTRIBUTED = "snowflake-distributed";

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(
                new JdbcConnectorFactory(
                        SNOWFLAKE_JDBC,
                        new SnowflakeJdbcClientModule(false)),
                new SnowflakeDistributedConnectorFactory(SNOWFLAKE_DISTRIBUTED));
    }
}
