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
package io.trino.spi.galaxy;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

public final class CatalogNetworkMonitor
{
    /**
     * Map of per-catalog network monitors. This data structure is static because there is no instance
     * local context available when using socket factories.
     */
    private static final ConcurrentMap<String, CatalogNetworkMonitor> CATALOG_NETWORK_MONITORS = new ConcurrentHashMap<>();
    private static final NetworkUsageQuotaEnforcer CROSS_REGION_NETWORK_USAGE_ENFORCER = new NetworkUsageQuotaEnforcer();

    public static CatalogNetworkMonitor getCatalogNetworkMonitor(String catalogName, String catalogId)
    {
        return CATALOG_NETWORK_MONITORS.computeIfAbsent(catalogId, id -> new CatalogNetworkMonitor(catalogName, id));
    }

    public static CatalogNetworkMonitor getCrossRegionCatalogNetworkMonitor(String catalogName, String catalogId, long maxCrossRegionReadBytes, long maxCrossRegionWriteBytes)
    {
        CROSS_REGION_NETWORK_USAGE_ENFORCER.checkLimitsAndThrowIfExceeded(maxCrossRegionReadBytes, maxCrossRegionWriteBytes);
        return CATALOG_NETWORK_MONITORS.computeIfAbsent(catalogId, id -> new CatalogNetworkMonitor(catalogName, id, maxCrossRegionReadBytes, maxCrossRegionWriteBytes));
    }

    public static Collection<CatalogNetworkMonitor> getAllCatalogNetworkMonitors()
    {
        return List.copyOf(CATALOG_NETWORK_MONITORS.values());
    }

    private final String catalogName;
    private final String catalogId;
    private final NetworkMonitor intraRegionMonitor = new NetworkMonitor();
    private final Optional<NetworkMonitor> crossRegionMonitor;

    private CatalogNetworkMonitor(String catalogName, String catalogId)
    {
        this(catalogName, catalogId, Optional.empty());
    }

    private CatalogNetworkMonitor(String catalogName, String catalogId, long maxCrossRegionReadBytes, long maxCrossRegionWriteBytes)
    {
        this(catalogName, catalogId, Optional.of(new QuotaEnforcingNetworkMonitor(CROSS_REGION_NETWORK_USAGE_ENFORCER, maxCrossRegionReadBytes, maxCrossRegionWriteBytes)));
    }

    private CatalogNetworkMonitor(String catalogName, String catalogId, Optional<NetworkMonitor> crossRegionMonitor)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.catalogId = requireNonNull(catalogId, "catalogId is null");
        this.crossRegionMonitor = requireNonNull(crossRegionMonitor, "crossRegionMonitor is null");
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getCatalogId()
    {
        return catalogId;
    }

    public long getIntraRegionReadBytes()
    {
        return intraRegionMonitor.getReadBytes();
    }

    public long getIntraRegionWriteBytes()
    {
        return intraRegionMonitor.getWriteBytes();
    }

    public long getCrossRegionReadBytes()
    {
        return crossRegionMonitor.map(NetworkMonitor::getReadBytes).orElse(0L);
    }

    public long getCrossRegionWriteBytes()
    {
        return crossRegionMonitor.map(NetworkMonitor::getWriteBytes).orElse(0L);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", CatalogNetworkMonitor.class.getSimpleName() + "[", "]")
                .add("catalogName=" + catalogName)
                .add("intraRegionMonitor=" + intraRegionMonitor)
                .add("crossRegionMonitor=" + crossRegionMonitor)
                .toString();
    }

    public InputStream monitorInputStream(CatalogConnectionType catalogConnectionType, InputStream inputStream)
    {
        return new MonitoredInputStream(getNetworkMonitor(catalogConnectionType), inputStream);
    }

    public OutputStream monitorOutputStream(CatalogConnectionType catalogConnectionType, OutputStream outputStream)
    {
        return new MonitoredOutputStream(getNetworkMonitor(catalogConnectionType), outputStream);
    }

    public void recordReadBytes(CatalogConnectionType catalogConnectionType, long bytes)
    {
        getNetworkMonitor(catalogConnectionType).recordReadBytes(bytes);
    }

    public void recordWriteBytes(CatalogConnectionType catalogConnectionType, long bytes)
    {
        getNetworkMonitor(catalogConnectionType).recordWriteBytes(bytes);
    }

    private NetworkMonitor getNetworkMonitor(CatalogConnectionType catalogConnectionType)
    {
        switch (catalogConnectionType) {
            case CROSS_REGION:
                return crossRegionMonitor.orElseThrow(
                        () -> new IllegalArgumentException("Cross-region querying is not allowed for catalog %s".formatted(catalogName)));
            case INTRA_REGION:
                return intraRegionMonitor;
            default:
                throw new IllegalArgumentException("Unknown catalog connection type: " + catalogConnectionType);
        }
    }
}
