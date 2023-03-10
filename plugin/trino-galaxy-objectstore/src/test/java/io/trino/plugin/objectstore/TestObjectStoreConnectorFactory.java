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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @deprecated TODO To be merged with {@link TestObjectStorePlugin}.
 */
@Deprecated
public class TestObjectStoreConnectorFactory
{
    @Test
    public void testCreateConnectorFailsWithUnusedConfig()
    {
        assertCreateConnectorFails(ImmutableMap.of("DELTA__unused_config", "somevalue"), "Configuration property 'unused_config' was not used");
        assertCreateConnectorFails(ImmutableMap.of("HIVE__unused_config", "somevalue"), "Configuration property 'unused_config' was not used");
        assertCreateConnectorFails(ImmutableMap.of("ICEBERG__unused_config", "somevalue"), "Configuration property 'unused_config' was not used");
        assertCreateConnectorFails(ImmutableMap.of("NOTEXISTS__hive.metastore.uri", "somevalue"), "Unused config: NOTEXISTS__hive.metastore.uri");
    }

    private static void assertCreateConnector(Map<String, String> properties)
    {
        ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.<String, String>builder()
                .put("OBJECTSTORE__object-store.table-type", "ICEBERG");
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            configBuilder.put(entry.getKey(), entry.getValue());
        }
        putAllTableTypes(configBuilder, "hive.metastore.uri", "thrift://localhost:1234");
        putAllTableTypes(configBuilder, "galaxy.account-url", "https://localhost:1234");
        Map<String, String> config = configBuilder.buildOrThrow();

        Connector connector = new ObjectStoreConnectorFactory("galaxy_objectstore")
                .create("test", config, new TestingConnectorContext());
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_UNCOMMITTED, true, true);
        connector.commit(transaction);
    }

    private static void assertCreateConnectorFails(Map<String, String> properties, String exceptionString)
    {
        assertThatThrownBy(() -> assertCreateConnector(properties))
                .hasMessageContaining(exceptionString);
    }

    private static void putAllTableTypes(ImmutableMap.Builder<String, String> builder, String key, String value)
    {
        ImmutableList.of("HIVE", "DELTA", "ICEBERG").forEach(
                type -> builder.put(String.format("%s__%s", type, key), value));
    }
}
