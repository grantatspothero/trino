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

import io.grpc.netty.shaded.io.netty.channel.ChannelFactory;
import io.grpc.netty.shaded.io.netty.channel.ReflectiveChannelFactory;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;

import static java.util.Objects.requireNonNull;

public class GalaxyForwardingChannelFactory
        implements ChannelFactory<NioSocketChannel>
{
    private final ChannelFactory<NioSocketChannel> delegate = new ReflectiveChannelFactory<>(NioSocketChannel.class);
    private final GalaxyMetricsRecordingChannelHandler sharedInboundChannelHandler;
    private final GalaxyMetricsRecordingChannelHandler sharedOutboundChannelHandler;

    public GalaxyForwardingChannelFactory(RegionVerifierProperties regionVerifierProperties, CatalogNetworkMonitorProperties catalogNetworkMonitorProperties)
    {
        requireNonNull(regionVerifierProperties, "regionVerifierProperties is null");
        requireNonNull(catalogNetworkMonitorProperties, "catalogNetworkMonitorProperties is null");
        this.sharedInboundChannelHandler = new GalaxyMetricsRecordingInboundChannelHandler(regionVerifierProperties, catalogNetworkMonitorProperties);
        this.sharedOutboundChannelHandler = new GalaxyMetricsRecordingOutboundChannelHandler(regionVerifierProperties, catalogNetworkMonitorProperties);
    }

    /**
     * Use separate inbound/outbound channel handlers so that the metrics can be recorded as close to sending the
     * message over the network as possible. This way the recorded metrics most closely resemble the actual size of
     * the data being sent/received.
     *
     * @see <a href="https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html">ChannelPipeline</a>
     */
    @Override
    public NioSocketChannel newChannel()
    {
        NioSocketChannel channel = delegate.newChannel();
        channel.pipeline().addFirst(sharedInboundChannelHandler);
        channel.pipeline().addLast(sharedOutboundChannelHandler);
        return channel;
    }
}
