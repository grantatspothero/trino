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
package io.trino.filesystem.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.tracing.opentelemetry.OpenTelemetryTracingOptions;
import com.azure.core.util.ConfigurationBuilder;
import com.azure.core.util.HttpClientOptions;
import com.azure.core.util.TracingOptions;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.azure.galaxy.GalaxyOkHttpAsyncClientProvider;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.security.ConnectorIdentity;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AzureFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final AzureAuth auth;
    private final DataSize readBlockSize;
    private final DataSize writeBlockSize;
    private final int maxWriteConcurrency;
    private final DataSize maxSingleUploadSize;
    private final TracingOptions tracingOptions;
    private final HttpClient httpClient;

    @Inject
    public AzureFileSystemFactory(
            OpenTelemetry openTelemetry,
            AzureAuth azureAuth,
            AzureFileSystemConfig config,
            CatalogHandle catalogHandle,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig)
    {
        this(openTelemetry,
                azureAuth,
                config.getReadBlockSize(),
                config.getWriteBlockSize(),
                config.getMaxWriteConcurrency(),
                config.getMaxSingleUploadSize(),
                catalogHandle,
                localRegionConfig,
                crossRegionConfig);
    }

    public AzureFileSystemFactory(
            OpenTelemetry openTelemetry,
            AzureAuth azureAuth,
            DataSize readBlockSize,
            DataSize writeBlockSize,
            int maxWriteConcurrency,
            DataSize maxSingleUploadSize,
            CatalogHandle catalogHandle,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig)
    {
        this.auth = requireNonNull(azureAuth, "azureAuth is null");
        this.readBlockSize = requireNonNull(readBlockSize, "readBlockSize is null");
        this.writeBlockSize = requireNonNull(writeBlockSize, "writeBlockSize is null");
        checkArgument(maxWriteConcurrency >= 0, "maxWriteConcurrency is negative");
        this.maxWriteConcurrency = maxWriteConcurrency;
        this.maxSingleUploadSize = requireNonNull(maxSingleUploadSize, "maxSingleUploadSize is null");

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        RegionVerifierProperties.addRegionVerifierProperties(configurationBuilder::putProperty, RegionVerifierProperties.generateFrom(localRegionConfig, crossRegionConfig));
        CatalogNetworkMonitorProperties catalogNetworkMonitorProperties = CatalogNetworkMonitorProperties.generateFrom(crossRegionConfig, catalogHandle)
                .withTlsEnabled(true);
        CatalogNetworkMonitorProperties.addCatalogNetworkMonitorProperties(configurationBuilder::putProperty, catalogNetworkMonitorProperties);

        this.tracingOptions = new OpenTelemetryTracingOptions().setOpenTelemetry(openTelemetry);
        this.httpClient = HttpClient.createDefault((HttpClientOptions) new HttpClientOptions()
                .setHttpClientProvider(GalaxyOkHttpAsyncClientProvider.class)
                .setConfiguration(configurationBuilder.build())
                .setTracingOptions(tracingOptions));
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new AzureFileSystem(httpClient, tracingOptions, auth, readBlockSize, writeBlockSize, maxWriteConcurrency, maxSingleUploadSize);
    }
}
