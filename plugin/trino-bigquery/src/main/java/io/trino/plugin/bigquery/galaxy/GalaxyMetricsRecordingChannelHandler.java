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
package io.trino.plugin.bigquery.galaxy;

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.grpc.netty.shaded.io.netty.channel.ChannelDuplexHandler;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandler.Sharable;
import io.grpc.netty.shaded.io.netty.util.AttributeKey;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.RegionVerifier;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.spi.galaxy.CatalogConnectionType;
import io.trino.spi.galaxy.CatalogNetworkMonitor;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * @see <a href="https://netty.io/4.1/api/io/netty/channel/ChannelHandler.html">ChannelHandler</a>
 * @see <a href="https://netty.io/4.1/api/io/netty/buffer/ByteBuf.html">ByteBuf</a>
 */
@Sharable
public class GalaxyMetricsRecordingChannelHandler
        extends ChannelDuplexHandler
{
    protected static final Logger log = Logger.get(GalaxyMetricsRecordingChannelHandler.class);

    protected final RegionVerifier regionVerifier;
    protected final String catalogName;
    protected final String catalogId;
    protected final boolean crossRegionAllowed;
    protected final Optional<DataSize> crossRegionReadLimit;
    protected final Optional<DataSize> crossRegionWriteLimit;
    protected final AttributeKey<CatalogConnectionType> catalogConnectionTypeAttributeKey = AttributeKey.valueOf("catalogConnectionType");

    public GalaxyMetricsRecordingChannelHandler(RegionVerifierProperties regionVerifierProperties, CatalogNetworkMonitorProperties catalogNetworkMonitorProperties)
    {
        regionVerifier = new RegionVerifier(regionVerifierProperties);
        requireNonNull(catalogNetworkMonitorProperties, "catalogNetworkMonitorProperties is null");
        catalogName = catalogNetworkMonitorProperties.catalogName();
        catalogId = catalogNetworkMonitorProperties.catalogId();
        crossRegionAllowed = catalogNetworkMonitorProperties.crossRegionAllowed();
        crossRegionReadLimit = catalogNetworkMonitorProperties.crossRegionReadLimit();
        crossRegionWriteLimit = catalogNetworkMonitorProperties.crossRegionWriteLimit();
    }

    protected CatalogNetworkMonitor getCatalogNetworkMonitor()
    {
        if (crossRegionAllowed) {
            verify(crossRegionReadLimit.isPresent(), "Cross-region read limit must be present to query cross-region catalog");
            verify(crossRegionWriteLimit.isPresent(), "Cross-region write limit must be present to query cross-region catalog");

            return CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, crossRegionReadLimit.get().toBytes(), crossRegionWriteLimit.get().toBytes());
        }
        return CatalogNetworkMonitor.getCatalogNetworkMonitor(catalogName, catalogId);
    }
}
