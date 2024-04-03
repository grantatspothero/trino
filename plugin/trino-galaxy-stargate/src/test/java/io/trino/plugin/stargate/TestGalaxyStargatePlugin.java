/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.trino.plugin.stargate;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Streams.stream;

public class TestGalaxyStargatePlugin
{
    @Test
    public void testCreateConnector()
    {
        createTestingPlugin(ImmutableMap.of("connection-url", "jdbc:trino://localhost:8080/test", "connection-user", "presto"));
    }

    public static void createTestingPlugin(Map<String, String> properties)
    {
        createTestingPlugin("stargate", properties);
    }

    public static void createTestingPlugin(String connectorName, Map<String, String> properties)
    {
        Plugin plugin = new TestingStargatePlugin(false);

        ConnectorFactory factory = stream(plugin.getConnectorFactories().iterator())
                .filter(connector -> connector.getName().equals(connectorName))
                .collect(toOptional())
                .orElseThrow();

        factory.create("test", properties, new TestingConnectorContext())
                .shutdown();
    }
}
