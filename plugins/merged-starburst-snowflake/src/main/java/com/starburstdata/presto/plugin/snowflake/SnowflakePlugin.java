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
import com.starburstdata.presto.license.LicenceCheckingConnectorFactory;
import com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeDistributedConnectorFactory;
import com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeJdbcClientModule;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

public class SnowflakePlugin
        implements Plugin
{
    static final String SNOWFLAKE_JDBC = "snowflake-jdbc";
    static final String SNOWFLAKE_DISTRIBUTED = "snowflake-distributed";

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(
                requireLicense(new JdbcConnectorFactory(
                        SNOWFLAKE_JDBC,
                        catalogName -> {
                            return new SnowflakeJdbcClientModule(catalogName, false);
                        })),
                requireLicense(new SnowflakeDistributedConnectorFactory(SNOWFLAKE_DISTRIBUTED)));
    }

    private static LicenceCheckingConnectorFactory requireLicense(ConnectorFactory connectorFactory)
    {
        return new LicenceCheckingConnectorFactory(connectorFactory, "snowflake");
    }
}