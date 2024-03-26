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
package io.trino.plugin.mongodb.galaxy;

import com.mongodb.MongoClientException;
import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.connection.BufferProvider;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.internal.connection.SocketStream;
import com.mongodb.internal.connection.SocketStreamFactory;
import com.mongodb.internal.connection.Stream;
import com.mongodb.spi.dns.InetAddressResolver;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sshtunnel.SshTunnelProperties;

import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties.addCatalogNetworkMonitorProperties;
import static io.trino.plugin.base.galaxy.RegionVerifierProperties.addRegionVerifierProperties;
import static io.trino.plugin.base.galaxy.RegionVerifierProperties.generateFrom;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;
import static java.util.Objects.requireNonNull;

public class GalaxySocketStreamFactory
        extends SocketStreamFactory
{
    private final SocketSettings socketSettings;
    private final SslSettings sslSettings;
    private final InetAddressResolver inetAddressResolver;
    private final BufferProvider bufferProvider = PowerOfTwoBufferPool.DEFAULT; // Value is same as com.mongodb.internal.connection.SocketStreamFactory#bufferProvider
    private final LocalRegionConfig localRegionConfig;
    private final CrossRegionConfig crossRegionConfig;
    private final Optional<SshTunnelProperties> sshTunnelProperties;
    private final CatalogHandle catalogHandle;

    public GalaxySocketStreamFactory(
            SocketSettings socketSettings,
            SslSettings sslSettings,
            InetAddressResolver inetAddressResolver,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig,
            Optional<SshTunnelProperties> sshTunnelProperties,
            CatalogHandle catalogHandle)
    {
        super(inetAddressResolver, socketSettings, sslSettings);
        this.socketSettings = requireNonNull(socketSettings, "settings is null");
        this.sslSettings = requireNonNull(sslSettings, "sslSettings is null");
        this.inetAddressResolver = requireNonNull(inetAddressResolver, "inetAddressResolver is null");
        this.localRegionConfig = requireNonNull(localRegionConfig, "localRegionConfig is null");
        this.crossRegionConfig = requireNonNull(crossRegionConfig, "crossRegionConfig is null");
        this.sshTunnelProperties = requireNonNull(sshTunnelProperties, "sshTunnelProperties is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
    }

    @Override
    public Stream create(ServerAddress serverAddress)
    {
        if (serverAddress instanceof UnixServerAddress) {
            throw new MongoClientException("Galaxy only supports IP sockets, not unix domain sockets");
        }
        Properties properties = new Properties();

        verify(!crossRegionConfig.getAllowCrossRegionAccess(), "Cross-region access not supported");
        addRegionVerifierProperties(properties::setProperty, generateFrom(localRegionConfig, crossRegionConfig));
        sshTunnelProperties.ifPresent(tunnelProperties -> addSshTunnelProperties(properties::setProperty, tunnelProperties));

        CatalogNetworkMonitorProperties catalogNetworkMonitorProperties = CatalogNetworkMonitorProperties.generateFrom(crossRegionConfig, catalogHandle)
                .withTlsEnabled(sslSettings.isEnabled());
        addCatalogNetworkMonitorProperties(properties::setProperty, catalogNetworkMonitorProperties);

        return new SocketStream(serverAddress, inetAddressResolver, socketSettings, sslSettings, new GalaxySqlSocketFactory(properties), bufferProvider);
    }
}
