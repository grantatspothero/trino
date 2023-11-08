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
package io.trino.plugin.snowflake;

import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.StreamSupport;

public class TestGalaxySnowflakePlugin
{
    @Test
    public void testCreateJdbcConnector()
    {
        findFactoryOrFail("snowflake_jdbc")
                .create(
                        "test",
                        Map.of(
                                "connection-url", "jdbc:snowflake:test",
                                "snowflake.role", "test",
                                "snowflake.database", "test",
                                "snowflake.warehouse", "test"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testCreateParallelConnector()
    {
        findFactoryOrFail("snowflake_parallel")
                .create(
                        "test",
                        Map.of(
                                "connection-url", "jdbc:snowflake:test",
                                "snowflake.role", "test",
                                "snowflake.database", "test",
                                "snowflake.warehouse", "test",
                                "galaxy.account-url", "https://whackadoodle.galaxy.com",
                                "snowflake.catalog-id", "c-1234567890"),
                        new TestingConnectorContext())
                .shutdown();
    }

    private static ConnectorFactory findFactoryOrFail(String name)
    {
        Iterable<ConnectorFactory> connectorFactories = new GalaxySnowflakePlugin().getConnectorFactories();
        return StreamSupport.stream(connectorFactories.spliterator(), false)
                .filter(f -> name.equals(f.getName()))
                .findAny()
                .orElseThrow(() -> new AssertionError("Factory with name '%s' not found".formatted(name)));
    }
}
