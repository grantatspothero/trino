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
package io.trino.plugin.galaxy.catalog;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGalaxyCatalogPlugin
{
    @Test
    public void testCreateGalaxyCatalogPlugin()
    {
        Plugin plugin = new GalaxyCatalogPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        ImmutableMap.Builder<String, String> propertiesMap = ImmutableMap.<String, String>builder().put("galaxy.account-url", "http://foo/bar.com");
        Connector connector = factory.create(GalaxyCatalogConnectorFactory.CONNECTOR_NAME, propertiesMap.buildOrThrow(), new TestingConnectorContext());
        assertThat(connector).isNotNull();
        connector.shutdown();
    }
}
