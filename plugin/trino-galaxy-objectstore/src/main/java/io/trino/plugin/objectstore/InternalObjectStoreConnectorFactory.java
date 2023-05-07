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
import io.trino.filesystem.hdfs.HdfsFileSystemModule;
import io.trino.hdfs.HdfsModule;
import io.trino.hdfs.authentication.HdfsAuthenticationModule;
import io.trino.hdfs.azure.HiveAzureModule;
import io.trino.hdfs.gcs.HiveGcsModule;
import io.trino.plugin.deltalake.InternalDeltaLakeConnectorFactory;
import io.trino.plugin.hive.InternalHiveConnectorFactory;
import io.trino.plugin.hive.s3.HiveS3Module;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.configuration.ConfigurationAwareModule.combine;

public final class InternalObjectStoreConnectorFactory
{
    private InternalObjectStoreConnectorFactory() {}

    public static Connector createConnector(String catalogName, Map<String, String> config, ConnectorContext context)
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
                            new GalaxyLocationSecurityModule()));

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
                            new GalaxyLocationSecurityModule()),
                    Optional.empty(),
                    Optional.empty());

            Map<String, String> deltaConfig = new HashMap<>(filteredConfig(config, "DELTA"));
            // The procedure is disabled in OSS because of security issues.
            // In Galaxy, they are addressed by location-based security and the procedure can be enabled by default.
            deltaConfig.putIfAbsent("delta.register-table-procedure.enabled", "true");
            Connector deltaConnector = InternalDeltaLakeConnectorFactory.createConnector(
                    catalogName,
                    deltaConfig,
                    context,
                    Optional.empty(),
                    Optional.empty(),
                    combine(
                            new ConfigureCachingMetastoreModule(),
                            new GalaxyLocationSecurityModule()));

            Connector hudiConnector = InternalHudiConnectorFactory.createConnector(
                    catalogName,
                    filteredConfig(config, "HUDI"),
                    context,
                    Optional.empty(),
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
                    new HdfsModule(),
                    new HiveS3Module(),
                    new HiveGcsModule(),
                    new HiveAzureModule(),
                    new HdfsAuthenticationModule(),
                    new HdfsFileSystemModule(),
                    binder -> {
                        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                        binder.bind(Tracer.class).toInstance(context.getTracer());
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
