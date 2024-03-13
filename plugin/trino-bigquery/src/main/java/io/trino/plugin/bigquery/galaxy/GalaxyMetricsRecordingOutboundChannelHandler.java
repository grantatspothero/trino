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
import io.grpc.netty.shaded.io.netty.channel.ChannelPromise;
import io.grpc.netty.shaded.io.netty.channel.socket.DatagramPacket;
import io.grpc.netty.shaded.io.netty.util.Attribute;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.spi.galaxy.CatalogConnectionType;

import java.net.SocketAddress;

import static io.trino.plugin.base.galaxy.InetAddresses.asInetSocketAddress;

public class GalaxyMetricsRecordingOutboundChannelHandler
        extends GalaxyMetricsRecordingChannelHandler
{
    public GalaxyMetricsRecordingOutboundChannelHandler(RegionVerifierProperties regionVerifierProperties, CatalogNetworkMonitorProperties catalogNetworkMonitorProperties)
    {
        super(regionVerifierProperties, catalogNetworkMonitorProperties);
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise)
            throws Exception
    {
        Attribute<CatalogConnectionType> catalogConnectionTypeAttr = ctx.channel().attr(catalogConnectionTypeAttributeKey);
        catalogConnectionTypeAttr.set(regionVerifier.getCatalogConnectionType("Database server", asInetSocketAddress(remoteAddress)));
        super.connect(ctx, remoteAddress, localAddress, promise);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception
    {
        CatalogConnectionType catalogConnectionType = ctx.channel().attr(catalogConnectionTypeAttributeKey).get();
        if (msg instanceof ByteBuf buffer) {
            if (buffer.readableBytes() > 0) {
                getCatalogNetworkMonitor().recordWriteBytes(catalogConnectionType, buffer.readableBytes());
            }
        }
        else if (msg instanceof DatagramPacket packet) {
            ByteBuf buffer = packet.content();
            if (buffer.readableBytes() > 0) {
                getCatalogNetworkMonitor().recordWriteBytes(catalogConnectionType, buffer.readableBytes());
            }
        }
        else {
            log.error("Unexpected type %s for parameter 'msg' when writing to BigQuery catalog %s".formatted(msg.getClass(), catalogId));
            throw new IllegalStateException("An unexpected error occurred when writing to BigQuery catalog %s".formatted(catalogName));
        }
        super.write(ctx, msg, promise);
    }
}
