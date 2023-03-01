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
package io.trino.plugin.warp;

import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.di.dispatcher.DispatcherWorkerDALModule;
import io.trino.plugin.varada.di.objectstore.WarpSpeedObjectStoreModule;
import io.trino.plugin.varada.dispatcher.DispatcherConnectorFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class WarpSpeedConnectorFactory
        implements ConnectorFactory
{
    public static final String WARP_PREFIX = "WARP__";
    private final DispatcherConnectorFactory dispatcherConnectorFactory;

    public static final String NAME = DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME;

    public WarpSpeedConnectorFactory(DispatcherConnectorFactory dispatcherConnectorFactory)
    {
        this.dispatcherConnectorFactory = requireNonNull(dispatcherConnectorFactory);
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Map<String, String> strippedConfig = Stream.concat(
                        config.entrySet().stream(),
                        Map.of(ProxiedConnectorConfiguration.PROXIED_CONNECTOR, "galaxy_objectstore",
                                        DispatcherWorkerDALModule.WORKER_DB_CONNECTION_PREFIX, "jdbc:hsqldb:mem:",
                                        DispatcherWorkerDALModule.WORKER_DB_CONNECTION_PATH, "workerDB/")
                                .entrySet()
                                .stream())
                .collect(toImmutableMap(
                        entry -> entry.getKey().startsWith(WARP_PREFIX) ? entry.getKey().substring(WARP_PREFIX.length()) : entry.getKey(),
                        Map.Entry::getValue));

        return dispatcherConnectorFactory.create(
                catalogName,
                strippedConfig,
                context,
                Optional.of(new WarpSpeedObjectStoreModule()),
                Map.of("galaxy_objectstore", new ObjectStoreProxyConnectorInitializer()));
    }
}
