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

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.socket.DatagramPacket;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.spi.galaxy.CatalogConnectionType;

public class GalaxyMetricsRecordingInboundChannelHandler
        extends GalaxyMetricsRecordingChannelHandler
{
    public GalaxyMetricsRecordingInboundChannelHandler(RegionVerifierProperties regionVerifierProperties, CatalogNetworkMonitorProperties catalogNetworkMonitorProperties)
    {
        super(regionVerifierProperties, catalogNetworkMonitorProperties);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
            throws Exception
    {
        CatalogConnectionType catalogConnectionType = ctx.channel().attr(catalogConnectionTypeAttributeKey).get();
        if (msg instanceof ByteBuf buffer) {
            if (buffer.readableBytes() > 0) {
                getCatalogNetworkMonitor().recordReadBytes(catalogConnectionType, buffer.readableBytes());
            }
        }
        else if (msg instanceof DatagramPacket packet) {
            ByteBuf buffer = packet.content();
            if (buffer.readableBytes() > 0) {
                getCatalogNetworkMonitor().recordReadBytes(catalogConnectionType, buffer.readableBytes());
            }
        }
        else {
            log.error("Unexpected type %s for parameter 'msg' when reading from BigQuery catalog %s".formatted(msg.getClass(), catalogId));
            throw new IllegalStateException("An unexpected error occurred when reading from BigQuery catalog %s".formatted(catalogId));
        }
        super.channelRead(ctx, msg);
    }
}
