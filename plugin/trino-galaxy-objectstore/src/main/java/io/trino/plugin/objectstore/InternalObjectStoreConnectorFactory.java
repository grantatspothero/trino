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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.deltalake.InternalDeltaLakeConnectorFactory;
import io.trino.plugin.hive.InternalHiveConnectorFactory;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hudi.InternalHudiConnectorFactory;
import io.trino.plugin.iceberg.InternalIcebergConnectorFactory;
import io.trino.plugin.objectstore.scheduler.GalaxyWorkSchedulerModule;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.type.TypeManager;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.configuration.ConfigurationAwareModule.combine;

public final class InternalObjectStoreConnectorFactory
{
    private InternalObjectStoreConnectorFactory() {}

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            Optional<HiveMetastore> hiveMetastore,
            Optional<TrinoFileSystemFactory> fileSystemFactory,
            Optional<Module> icebergCatalogModule,
            Optional<Module> deltaMetastoreModule,
            Module deltaModule,
            ConnectorContext context)
    {
        ClassLoader classLoader = InternalObjectStoreConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            verifyConfigPrefix(config);

            Connector hiveConnector = InternalHiveConnectorFactory.createConnector(
                    catalogName,
                    filteredConfig(config, "HIVE"),
                    context,
                    combine(
                            new ConfigureCachingMetastoreModule(),
                            new GalaxyLocationSecurityModule()),
                    hiveMetastore,
                    fileSystemFactory,
                    Optional.empty());

            Map<String, String> icebergConfig = new HashMap<>(filteredConfig(config, "ICEBERG"));
            // The procedure is disabled in OSS because of security issues.
            // In Galaxy, they are addressed by location-based security and the procedure can be enabled by default.
            icebergConfig.putIfAbsent("iceberg.register-table-procedure.enabled", "true");
            Connector icebergConnector = InternalIcebergConnectorFactory.createConnector(
                    catalogName,
                    icebergConfig,
                    context,
                    combine(
                            new ConfigureCachingMetastoreModule(),
                            new GalaxyLocationSecurityModule(),
                            new GalaxyWorkSchedulerModule()),
                    icebergCatalogModule,
                    fileSystemFactory);

            Map<String, String> deltaConfig = new HashMap<>(filteredConfig(config, "DELTA"));
            // The procedure is disabled in OSS because of security issues.
            // In Galaxy, they are addressed by location-based security and the procedure can be enabled by default.
            deltaConfig.putIfAbsent("delta.register-table-procedure.enabled", "true");
            Connector deltaConnector = InternalDeltaLakeConnectorFactory.createConnector(
                    catalogName,
                    deltaConfig,
                    context,
                    deltaMetastoreModule,
                    fileSystemFactory,
                    combine(
                            new ConfigureCachingMetastoreModule(),
                            new GalaxyLocationSecurityModule(),
                            deltaModule));

            Connector hudiConnector = InternalHudiConnectorFactory.createConnector(
                    catalogName,
                    filteredConfig(config, "HUDI"),
                    hiveMetastore,
                    context,
                    Optional.of(combine(
                            new ConfigureCachingMetastoreModule(),
                            new GalaxyLocationSecurityModule())));

            Bootstrap app = new Bootstrap(
                    connectorModule(ForHive.class, hiveConnector),
                    connectorModule(ForIceberg.class, icebergConnector),
                    connectorModule(ForDelta.class, deltaConnector),
                    connectorModule(ForHudi.class, hudiConnector),
                    new ObjectStoreModule(),
                    new GalaxyLocationSecurityModule(),
                    new FileSystemModule(catalogName, context.getNodeManager(), context.getOpenTelemetry()),
                    new GalaxyWorkSchedulerModule(),
                    binder -> {
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                        binder.bind(Tracer.class).toInstance(context.getTracer());
                        binder.bind(CatalogHandle.class).toInstance(context.getCatalogHandle());
                        configBinder(binder).bindConfig(LocalRegionConfig.class);
                        configBinder(binder).bindConfig(CrossRegionConfig.class);
                    });

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
