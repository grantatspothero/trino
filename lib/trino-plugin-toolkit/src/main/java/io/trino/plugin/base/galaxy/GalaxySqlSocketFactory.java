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

import static io.trino.plugin.base.galaxy.InetAddresses.asInetSocketAddress;
import static io.trino.plugin.base.galaxy.InetAddresses.toInetAddresses;
import static io.trino.spi.galaxy.CatalogNetworkMonitor.checkCrossRegionLimitsAndThrowIfExceeded;
import static io.trino.spi.galaxy.CatalogNetworkMonitor.getCatalogNetworkMonitor;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.getOptionalProperty;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.getRequiredProperty;
import static java.util.Objects.requireNonNull;

public class GalaxySqlSocketFactory
        extends SocketFactory
{
    public static final String CATALOG_NAME_PROPERTY_NAME = "catalogName";
    public static final String CATALOG_ID_PROPERTY_NAME = "catalogId";
    // tlsEnabled is only needed for the mongo socket factory currently
    // JDBC drivers wrap the socket in an SSLSocket if necessary
    public static final String TLS_ENABLED_PROPERTY_NAME = "tlsEnabled";
    private static final String CROSS_REGION_READ_LIMIT_PROPERTY_NAME = "crossRegionReadLimit";
    private static final String CROSS_REGION_WRITE_LIMIT_PROPERTY_NAME = "crossRegionWriteLimit";

    private final String catalogName;
    private final String catalogId;
    private final Optional<SshTunnelManager> sshTunnelManager;
    private final boolean tlsEnabled;
    private final RegionVerifier regionVerifier;
    private final Optional<DataSize> crossRegionReadLimit;
    private final Optional<DataSize> crossRegionWriteLimit;

    public GalaxySqlSocketFactory(Properties properties)
    {
        requireNonNull(properties, "properties is null");
        catalogName = getCatalogName(properties);
        catalogId = getCatalogId(properties);
        sshTunnelManager = getSshTunnelProperties(properties)
                .map(SshTunnelManager::getCached);
        tlsEnabled = getOptionalProperty(properties, TLS_ENABLED_PROPERTY_NAME)
                .map(Boolean::parseBoolean)
                .orElse(false);
        regionVerifier = new RegionVerifier(RegionVerifierProperties.getRegionVerifierProperties(properties::getProperty));
        crossRegionReadLimit = getOptionalProperty(properties, CROSS_REGION_READ_LIMIT_PROPERTY_NAME).map(DataSize::valueOf);
        crossRegionWriteLimit = getOptionalProperty(properties, CROSS_REGION_WRITE_LIMIT_PROPERTY_NAME).map(DataSize::valueOf);
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
        private boolean isCrossRegion;

        public SocketAddress redirectAddress(SocketAddress socketAddress)
        {
            InetSocketAddress inetSocketAddress = asInetSocketAddress(socketAddress);

            if (sshTunnelManager.isPresent()) {
                SshTunnelManager tunnelManager = sshTunnelManager.get();
                Tunnel tunnel = tunnelManager.getOrCreateTunnel(HostAndPort.fromParts(inetSocketAddress.getHostString(), inetSocketAddress.getPort()));
                // Verify that the SSH server will be within the region-local IP ranges (because it will be our first network hop)
                isCrossRegion = regionVerifier.isCrossRegionAccess("SSH tunnel server", toInetAddresses(tunnelManager.getSshServer().getHost()));
                return new InetSocketAddress("127.0.0.1", tunnel.getLocalTunnelPort());
            }

            isCrossRegion = regionVerifier.isCrossRegionAccess("Database server", inetSocketAddress);
            return inetSocketAddress;
        }

        public InputStream getInputStream(InputStream inputStream)
                throws IOException
        {
            long crossRegionReadLimitBytes = crossRegionReadLimit.orElseGet(() -> DataSize.ofBytes(0)).toBytes();
            long crossRegionWriteLimitBytes = crossRegionWriteLimit.orElseGet(() -> DataSize.ofBytes(0)).toBytes();

            if (isCrossRegion) {
                checkCrossRegionLimitsAndThrowIfExceeded(crossRegionReadLimitBytes, crossRegionWriteLimitBytes);
            }
            return getCatalogNetworkMonitor(catalogName, catalogId, crossRegionReadLimitBytes, crossRegionWriteLimitBytes).monitorInputStream(isCrossRegion, inputStream);
        }

        public OutputStream getOutputStream(OutputStream outputStream)
                throws IOException
        {
            long crossRegionReadLimitBytes = crossRegionReadLimit.orElseGet(() -> DataSize.ofBytes(0)).toBytes();
            long crossRegionWriteLimitBytes = crossRegionWriteLimit.orElseGet(() -> DataSize.ofBytes(0)).toBytes();

            if (isCrossRegion) {
                checkCrossRegionLimitsAndThrowIfExceeded(crossRegionReadLimitBytes, crossRegionWriteLimitBytes);
            }
            return getCatalogNetworkMonitor(catalogName, catalogId, crossRegionReadLimitBytes, crossRegionWriteLimitBytes).monitorOutputStream(isCrossRegion, outputStream);
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

    public static void addCatalogName(Properties properties, String catalogName)
    {
        properties.setProperty(CATALOG_NAME_PROPERTY_NAME, catalogName);
    }

    private static String getCatalogName(Properties properties)
    {
        return getRequiredProperty(properties, CATALOG_NAME_PROPERTY_NAME);
    }

    public static void addCatalogId(Properties properties, String catalogId)
    {
        properties.setProperty(CATALOG_ID_PROPERTY_NAME, catalogId);
    }

    private static String getCatalogId(Properties properties)
    {
        return getRequiredProperty(properties, CATALOG_ID_PROPERTY_NAME);
    }

    public static void addTlsEnabled(Properties properties)
    {
        properties.setProperty(TLS_ENABLED_PROPERTY_NAME, "true");
    }

    public static void addCrossRegionReadLimit(Properties properties, DataSize crossRegionReadLimit)
    {
        properties.setProperty(CROSS_REGION_READ_LIMIT_PROPERTY_NAME, crossRegionReadLimit.toBytesValueString());
    }

    public static void addCrossRegionWriteLimit(Properties properties, DataSize crossRegionWriteLimit)
    {
        properties.setProperty(CROSS_REGION_WRITE_LIMIT_PROPERTY_NAME, crossRegionWriteLimit.toBytesValueString());
    }

    private static Optional<SshTunnelProperties> getSshTunnelProperties(Properties properties)
    {
        return SshTunnelPropertiesMapper.getSshTunnelProperties(name -> getOptionalProperty(properties, name));
    }
}
