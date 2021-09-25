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

    public static CatalogNetworkMonitor getCatalogNetworkMonitor(String catalogName, String catalogId)
    {
        return CATALOG_NETWORK_MONITORS.computeIfAbsent(catalogId, id -> new CatalogNetworkMonitor(catalogName, id));
    }

    public static Collection<CatalogNetworkMonitor> getAllCatalogNetworkMonitors()
    {
        return List.copyOf(CATALOG_NETWORK_MONITORS.values());
    }

    private final String catalogName;
    private final String catalogId;
    private final NetworkMonitor intraRegionMonitor = new NetworkMonitor();
    private final NetworkMonitor crossRegionMonitor = new NetworkMonitor();

    private CatalogNetworkMonitor(String catalogName, String catalogId)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.catalogId = requireNonNull(catalogId, "catalogId is null");
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
        return crossRegionMonitor.getReadBytes();
    }

    public long getCrossRegionWriteBytes()
    {
        return crossRegionMonitor.getWriteBytes();
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", CatalogNetworkMonitor.class.getSimpleName() + "[", "]")
                .add("catalogName=" + catalogName)
                .add("intraRegion=" + intraRegionMonitor)
                .add("crossRegion=" + crossRegionMonitor)
                .toString();
    }

    public InputStream monitorInputStream(boolean crossRegion, InputStream inputStream)
    {
        return new MonitoredInputStream(crossRegion ? crossRegionMonitor : intraRegionMonitor, inputStream);
    }

    public OutputStream monitorOutputStream(boolean crossRegion, OutputStream outputStream)
    {
        return new MonitoredOutputStream(crossRegion ? crossRegionMonitor : intraRegionMonitor, outputStream);
    }
}
