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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.di.dispatcher.DispatcherWorkerDALModule;
import io.trino.plugin.varada.di.objectstore.WarpSpeedObjectStoreModule;
import io.trino.plugin.varada.dispatcher.DispatcherConnectorFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class WarpSpeedConnectorFactory
        implements ConnectorFactory
{
    public static final String WARP_PREFIX = "WARP__";
    private final DispatcherConnectorFactory dispatcherConnectorFactory;

    private static final String NAME = DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME;

    public WarpSpeedConnectorFactory(DispatcherConnectorFactory dispatcherConnectorFactory)
    {
        this.dispatcherConnectorFactory = requireNonNull(dispatcherConnectorFactory);
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Optional<List<Class>> optionalModules = Optional.of(List.of(WarpSpeedObjectStoreModule.class));
        Map<String, String> extraConfig = Map.of(
                DispatcherWorkerDALModule.WORKER_DB_CONNECTION_PREFIX, "jdbc:hsqldb:mem:",
                DispatcherWorkerDALModule.WORKER_DB_CONNECTION_PATH, "workerDB/");

        String proxyConnectorName = config.getOrDefault(ProxiedConnectorConfiguration.PROXIED_CONNECTOR,
                ObjectStoreProxyConnectorInitializer.CONNECTOR_NAME);

        if (proxyConnectorName.equals(ProxiedConnectorConfiguration.ICEBERG_CONNECTOR_NAME)) {
            // support for Tabular which should act as warp-speed iceberg.
            ImmutableMap.Builder<String, String> strippedConfig = ImmutableMap.builder();
            strippedConfig.putAll(config);
            strippedConfig.putAll(extraConfig);

            return dispatcherConnectorFactory.create(
                    catalogName,
                    strippedConfig.buildOrThrow(),
                    context,
                    optionalModules,
                    Map.of(ProxiedConnectorConfiguration.ICEBERG_CONNECTOR_NAME,
                            IcebergProxiedConnectorInitializer.class.getName()),
                    true);
        }
        else {
            Map<String, String> strippedConfig =
                    Stream.of(config.entrySet(),
                                    Map.of(ProxiedConnectorConfiguration.PROXIED_CONNECTOR, "galaxy_objectstore").entrySet(),
                                    extraConfig.entrySet())
                            .flatMap(Set::stream)
                            .collect(toImmutableMap(
                                    entry -> entry.getKey().startsWith(WARP_PREFIX) ? entry.getKey().substring(WARP_PREFIX.length()) : entry.getKey(),
                                    Map.Entry::getValue));

            return dispatcherConnectorFactory.create(
                    catalogName,
                    strippedConfig,
                    context,
                    optionalModules,
                    Map.of(ObjectStoreProxyConnectorInitializer.CONNECTOR_NAME,
                            ObjectStoreProxyConnectorInitializer.class.getName()));
        }
    }
}