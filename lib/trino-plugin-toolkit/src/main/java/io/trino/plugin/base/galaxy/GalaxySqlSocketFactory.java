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
package io.trino.plugin.base.galaxy;

import com.google.common.net.HostAndPort;
import io.airlift.units.DataSize;
import io.trino.spi.galaxy.CatalogConnectionType;
import io.trino.sshtunnel.SshTunnelManager;
import io.trino.sshtunnel.SshTunnelManager.Tunnel;
import io.trino.sshtunnel.SshTunnelProperties;
import io.trino.sshtunnel.SshTunnelPropertiesMapper;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.galaxy.InetAddresses.asInetSocketAddress;
import static io.trino.plugin.base.galaxy.InetAddresses.toInetAddresses;
import static io.trino.spi.galaxy.CatalogNetworkMonitor.getCatalogNetworkMonitor;
import static io.trino.spi.galaxy.CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.getOptionalProperty;
import static java.util.Objects.requireNonNull;

public class GalaxySqlSocketFactory
        extends SocketFactory
{
    private final Optional<SshTunnelManager> sshTunnelManager;
    private final RegionVerifier regionVerifier;
    private final String catalogName;
    private final String catalogId;
    private final boolean tlsEnabled;
    private final boolean crossRegionAllowed;
    private final Optional<DataSize> crossRegionReadLimit;
    private final Optional<DataSize> crossRegionWriteLimit;

    public GalaxySqlSocketFactory(Properties properties)
    {
        requireNonNull(properties, "properties is null");
        sshTunnelManager = getSshTunnelProperties(properties)
                .map(SshTunnelManager::getCached);
        regionVerifier = new RegionVerifier(RegionVerifierProperties.getRegionVerifierProperties(properties::getProperty));
        CatalogNetworkMonitorProperties catalogNetworkMonitorProperties = CatalogNetworkMonitorProperties.getCatalogNetworkMonitorProperties(propertyName -> getOptionalProperty(properties, propertyName));
        catalogName = catalogNetworkMonitorProperties.catalogName();
        catalogId = catalogNetworkMonitorProperties.catalogId();
        tlsEnabled = catalogNetworkMonitorProperties.tlsEnabled();
        crossRegionAllowed = catalogNetworkMonitorProperties.crossRegionAllowed();
        crossRegionReadLimit = catalogNetworkMonitorProperties.crossRegionReadLimit();
        crossRegionWriteLimit = catalogNetworkMonitorProperties.crossRegionWriteLimit();
    }

    private SSLSocket createSSLSocket()
    {
        try {
            return (SSLSocket) SSLContext.getDefault().getSocketFactory().createSocket();
        }
        catch (NoSuchAlgorithmException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    class SocketWrapperAndVerifier
    {
        private CatalogConnectionType catalogConnectionType;

        public SocketAddress redirectAddress(SocketAddress socketAddress)
        {
            InetSocketAddress inetSocketAddress = asInetSocketAddress(socketAddress);

            if (sshTunnelManager.isPresent()) {
                SshTunnelManager tunnelManager = sshTunnelManager.get();
                Tunnel tunnel = tunnelManager.getOrCreateTunnel(HostAndPort.fromParts(inetSocketAddress.getHostString(), inetSocketAddress.getPort()));
                // Verify that the SSH server will be within the region-local IP ranges (because it will be our first network hop)
                catalogConnectionType = regionVerifier.getCatalogConnectionType("SSH tunnel server", toInetAddresses(tunnelManager.getSshServer().getHost()));
                return new InetSocketAddress("127.0.0.1", tunnel.getLocalTunnelPort());
            }

            catalogConnectionType = regionVerifier.getCatalogConnectionType("Database server", inetSocketAddress);
            return inetSocketAddress;
        }

        public InputStream getInputStream(InputStream inputStream)
                throws IOException
        {
            if (crossRegionAllowed) {
                verify(crossRegionReadLimit.isPresent(), "Cross-region read limit must be present to query cross-region catalog");
                verify(crossRegionWriteLimit.isPresent(), "Cross-region write limit must be present to query cross-region catalog");

                return getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, crossRegionReadLimit.get().toBytes(), crossRegionWriteLimit.get().toBytes()).monitorInputStream(catalogConnectionType, inputStream);
            }
            return getCatalogNetworkMonitor(catalogName, catalogId).monitorInputStream(catalogConnectionType, inputStream);
        }

        public OutputStream getOutputStream(OutputStream outputStream)
                throws IOException
        {
            if (crossRegionAllowed) {
                verify(crossRegionReadLimit.isPresent(), "Cross-region read limit must be present to query cross-region catalog");
                verify(crossRegionWriteLimit.isPresent(), "Cross-region write limit must be present to query cross-region catalog");

                return getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, crossRegionReadLimit.get().toBytes(), crossRegionWriteLimit.get().toBytes()).monitorOutputStream(catalogConnectionType, outputStream);
            }
            return getCatalogNetworkMonitor(catalogName, catalogId).monitorOutputStream(catalogConnectionType, outputStream);
        }
    }

    @Override
    public Socket createSocket()
    {
        SocketWrapperAndVerifier socketWrapper = new SocketWrapperAndVerifier();
        if (tlsEnabled) {
            return new RedirectingSSLSocket(createSSLSocket(), socketWrapper);
        }
        else {
            return new RedirectingSocket(socketWrapper);
        }
    }

    @Override
    public Socket createSocket(String host, int port)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress host, int port)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
    {
        throw new UnsupportedOperationException();
    }

    private static Optional<SshTunnelProperties> getSshTunnelProperties(Properties properties)
    {
        return SshTunnelPropertiesMapper.getSshTunnelProperties(name -> getOptionalProperty(properties, name));
    }
}
