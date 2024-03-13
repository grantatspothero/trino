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
package io.trino.filesystem.azure.galaxy;

import io.airlift.units.DataSize;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.RegionVerifier;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.spi.galaxy.CatalogConnectionType;
import io.trino.spi.galaxy.CatalogNetworkMonitor;
import jakarta.validation.constraints.NotNull;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.galaxy.InetAddresses.toInetAddresses;
import static java.util.Objects.requireNonNull;

public class GalaxyNetworkInterceptor
        implements Interceptor
{
    private final RegionVerifier regionVerifier;
    private final String catalogName;
    private final String catalogId;
    private final boolean crossRegionAllowed;
    private final Optional<DataSize> crossRegionReadLimit;
    private final Optional<DataSize> crossRegionWriteLimit;

    public GalaxyNetworkInterceptor(RegionVerifierProperties regionVerifierProperties, CatalogNetworkMonitorProperties catalogNetworkMonitorProperties)
    {
        regionVerifier = new RegionVerifier(regionVerifierProperties);
        requireNonNull(catalogNetworkMonitorProperties, "catalogNetworkMonitorProperties is null");
        catalogName = catalogNetworkMonitorProperties.catalogName();
        catalogId = catalogNetworkMonitorProperties.catalogId();
        crossRegionAllowed = catalogNetworkMonitorProperties.crossRegionAllowed();
        crossRegionReadLimit = catalogNetworkMonitorProperties.crossRegionReadLimit();
        crossRegionWriteLimit = catalogNetworkMonitorProperties.crossRegionWriteLimit();
    }

    @NotNull
    @Override
    public Response intercept(@NotNull Interceptor.Chain chain)
            throws IOException
    {
        Request request = chain.request();
        CatalogConnectionType catalogConnectionType = regionVerifier.getCatalogConnectionType("Database server", toInetAddresses(request.url().host()));
        CatalogNetworkMonitor networkMonitor = getCatalogNetworkMonitor();

        Response response = chain.proceed(request);
        if (response.isSuccessful()) {
            if (request.body() != null) {
                networkMonitor.recordWriteBytes(catalogConnectionType, request.body().contentLength());
            }
            if (response.body() != null) {
                networkMonitor.recordReadBytes(catalogConnectionType, response.body().contentLength());
            }
        }
        return response;
    }

    private CatalogNetworkMonitor getCatalogNetworkMonitor()
    {
        if (crossRegionAllowed) {
            verify(crossRegionReadLimit.isPresent(), "Cross-region read limit must be present to query cross-region catalog");
            verify(crossRegionWriteLimit.isPresent(), "Cross-region write limit must be present to query cross-region catalog");

            return CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, crossRegionReadLimit.get().toBytes(), crossRegionWriteLimit.get().toBytes());
        }
        return CatalogNetworkMonitor.getCatalogNetworkMonitor(catalogName, catalogId);
    }
}
