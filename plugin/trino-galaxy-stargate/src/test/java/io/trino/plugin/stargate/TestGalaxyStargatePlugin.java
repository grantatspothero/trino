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

        factory.create("test", properties, new TestingConnectorContext());
    }
}
