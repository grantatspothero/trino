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

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.deltalake.InternalDeltaLakeConnectorFactory;
import io.trino.plugin.hive.InternalHiveConnectorFactory;
import io.trino.plugin.hudi.InternalHudiConnectorFactory;
import io.trino.plugin.iceberg.InternalIcebergConnectorFactory;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public final class InternalObjectStoreConnectorFactory
{
    private InternalObjectStoreConnectorFactory() {}

    @SuppressWarnings("unused")
    public static Connector createConnector(String catalogName, Map<String, String> config, ConnectorContext context, Module module)
    {
        ClassLoader classLoader = InternalObjectStoreConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            verifyConfigPrefix(config);

            Connector hiveConnector = InternalHiveConnectorFactory.createConnector(
                    catalogName,
                    filteredConfig(config, "HIVE"),
                    context,
                    new GalaxyLocationSecurityModule());

            Connector icebergConnector = InternalIcebergConnectorFactory.createConnector(
                    catalogName,
                    filteredConfig(config, "ICEBERG"),
                    context,
                    new GalaxyLocationSecurityModule(),
                    Optional.empty(),
                    Optional.empty());

            Connector deltaConnector = InternalDeltaLakeConnectorFactory.createConnector(
                    catalogName,
                    filteredConfig(config, "DELTA"),
                    context,
                    Optional.empty(),
                    new GalaxyLocationSecurityModule());

            Connector hudiConnector = InternalHudiConnectorFactory.createConnector(
                    catalogName,
                    filteredConfig(config, "HUDI"),
                    context,
                    Optional.empty(),
                    Optional.of(new GalaxyLocationSecurityModule()));

            Bootstrap app = new Bootstrap(
                    connectorModule(ForHive.class, hiveConnector),
                    connectorModule(ForIceberg.class, icebergConnector),
                    connectorModule(ForDelta.class, deltaConnector),
                    connectorModule(ForHudi.class, hudiConnector),
                    new ObjectStoreModule(),
                    new GalaxyLocationSecurityModule());

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(filteredConfig(config, "OBJECTSTORE"))
                    .initialize();

            return injector.getInstance(ObjectStoreConnector.class);
        }
    }

    private static Module connectorModule(Class<? extends Annotation> annotation, Connector connector)
    {
        return binder -> {
            binder.bind(Connector.class).annotatedWith(annotation).toInstance(connector);
            binder.bind(ConnectorSplitManager.class).annotatedWith(annotation).toInstance(connector.getSplitManager());
            binder.bind(ConnectorPageSourceProvider.class).annotatedWith(annotation).toInstance(connector.getPageSourceProvider());
            binder.bind(ConnectorPageSinkProvider.class).annotatedWith(annotation).toInstance(connector.getPageSinkProvider());
            binder.bind(ConnectorNodePartitioningProvider.class).annotatedWith(annotation).toInstance(connector.getNodePartitioningProvider());
        };
    }

    private static void verifyConfigPrefix(Map<String, String> config)
    {
        config.keySet().stream()
                .filter(key -> !key.startsWith("OBJECTSTORE__"))
                .filter(key -> !key.startsWith("HIVE__"))
                .filter(key -> !key.startsWith("ICEBERG__"))
                .filter(key -> !key.startsWith("DELTA__"))
                .filter(key -> !key.startsWith("HUDI__"))
                .findAny().ifPresent(key -> {
                    throw new IllegalArgumentException("Unused config: " + key);
                });
    }

    private static Map<String, String> filteredConfig(Map<String, String> config, String prefix)
    {
        String start = prefix + "__";
        return config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(start))
                .collect(toImmutableMap(
                        entry -> entry.getKey().substring(start.length()),
                        Map.Entry::getValue));
    }
}
