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
package io.trino.plugin.bigquery;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.http.HttpTransportOptions;
import com.google.inject.Inject;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.plugin.bigquery.galaxy.GalaxyForwardingChannelFactory;
import io.trino.plugin.bigquery.galaxy.GalaxySSLSocketFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class GalaxyTransportChannelProviderConfigurer
        implements BigQueryOptionsConfigurer
{
    private final RegionVerifierProperties regionVerifierProperties;
    private final CatalogNetworkMonitorProperties catalogNetworkMonitorProperties;

    @Inject
    public GalaxyTransportChannelProviderConfigurer(
            CatalogHandle catalogHandle,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(localRegionConfig, "localRegionConfig is null");
        requireNonNull(crossRegionConfig, "crossRegionConfig is null");

        verify(crossRegionConfig.getAllowCrossRegionAccess(), "Cross-region access must be enabled for BigQuery");
        regionVerifierProperties = RegionVerifierProperties.generateFrom(localRegionConfig, crossRegionConfig);
        catalogNetworkMonitorProperties = CatalogNetworkMonitorProperties.generateFrom(crossRegionConfig, catalogHandle)
                .withTlsEnabled(true);
    }

    @Override
    public BigQueryOptions.Builder configure(BigQueryOptions.Builder builder, ConnectorSession session)
    {
        builder.setTransportOptions(HttpTransportOptions.newBuilder()
                .setHttpTransportFactory(() -> new NetHttpTransport.Builder()
                        .setSslSocketFactory(new GalaxySSLSocketFactory(regionVerifierProperties, catalogNetworkMonitorProperties))
                        .build())
                .build());
        return builder;
    }

    /**
     * ChannelFactory and EventLoopGroup defaults pulled from the configuration used in the grpc-java netty project.
     *
     * @see <a href="https://github.com/grpc/grpc-java/blob/ed73755da838d66f06d5ab57464fa553bfe24001/netty/src/main/java/io/grpc/netty/Utils.java#L127">DEFAULT_CLIENT_CHANNEL_TYPE</a>
     * @see <a href="https://github.com/grpc/grpc-java/blob/ed73755da838d66f06d5ab57464fa553bfe24001/netty/src/main/java/io/grpc/netty/Utils.java#L417">DefaultEventLoopGroupResource</a>
     */
    @Override
    public BigQueryReadSettings.Builder configure(BigQueryReadSettings.Builder builder, ConnectorSession session)
    {
        builder.setTransportChannelProvider(InstantiatingGrpcChannelProvider.newBuilder()
                .setMaxInboundMessageSize(Integer.MAX_VALUE)
                .setChannelConfigurator(channelBuilder -> {
                    checkState(channelBuilder instanceof NettyChannelBuilder, "Expected ManagedChannelBuilder to be an instance of NettyChannelBuilder");
                    NettyChannelBuilder nettyChannelBuilder = (NettyChannelBuilder) channelBuilder;
                    nettyChannelBuilder.channelFactory(new GalaxyForwardingChannelFactory(regionVerifierProperties, catalogNetworkMonitorProperties));
                    nettyChannelBuilder.eventLoopGroup(new NioEventLoopGroup(0, new DefaultThreadFactory("grpc-nio-worker-ELG", true)));
                    return nettyChannelBuilder;
                })
                .build());
        return builder;
    }

    @Override
    public BigQueryWriteSettings.Builder configure(BigQueryWriteSettings.Builder builder, ConnectorSession session)
    {
        builder.setTransportChannelProvider(InstantiatingGrpcChannelProvider.newBuilder()
                .setMaxInboundMessageSize(Integer.MAX_VALUE)
                .setChannelConfigurator(channelBuilder -> {
                    checkState(channelBuilder instanceof NettyChannelBuilder, "Expected ManagedChannelBuilder to be an instance of NettyChannelBuilder");
                    NettyChannelBuilder nettyChannelBuilder = (NettyChannelBuilder) channelBuilder;
                    nettyChannelBuilder.channelFactory(new GalaxyForwardingChannelFactory(regionVerifierProperties, catalogNetworkMonitorProperties));
                    nettyChannelBuilder.eventLoopGroup(new NioEventLoopGroup(0, new DefaultThreadFactory("grpc-nio-worker-ELG", true)));
                    return nettyChannelBuilder;
                })
                .build());
        return builder;
    }
}
