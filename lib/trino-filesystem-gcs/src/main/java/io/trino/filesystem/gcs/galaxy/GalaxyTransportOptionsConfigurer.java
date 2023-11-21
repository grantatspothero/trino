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
package io.trino.filesystem.gcs.galaxy;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions.Builder;
import com.google.inject.Inject;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.GalaxySSLSocketFactory;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.spi.connector.CatalogHandle;

import static java.util.Objects.requireNonNull;

public class GalaxyTransportOptionsConfigurer
        implements StorageOptionsConfigurer
{
    private final RegionVerifierProperties regionVerifierProperties;
    private final CatalogNetworkMonitorProperties catalogNetworkMonitorProperties;

    @Inject
    public GalaxyTransportOptionsConfigurer(
            CatalogHandle catalogHandle,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(localRegionConfig, "localRegionConfig is null");
        requireNonNull(crossRegionConfig, "crossRegionConfig is null");

        regionVerifierProperties = RegionVerifierProperties.generateFrom(localRegionConfig, crossRegionConfig);
        catalogNetworkMonitorProperties = CatalogNetworkMonitorProperties.generateFrom(crossRegionConfig, catalogHandle)
                .withTlsEnabled(true);
    }

    @Override
    public Builder configure(Builder builder)
    {
        builder.setTransportOptions(HttpTransportOptions.newBuilder()
                .setHttpTransportFactory(() -> new NetHttpTransport.Builder()
                        .setSslSocketFactory(new GalaxySSLSocketFactory(regionVerifierProperties, catalogNetworkMonitorProperties))
                        .build())
                .build());
        return builder;
    }
}
