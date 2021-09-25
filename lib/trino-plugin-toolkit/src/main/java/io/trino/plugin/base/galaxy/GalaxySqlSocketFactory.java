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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.net.HostAndPort;
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
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.InetAddresses.toAddrString;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.base.galaxy.InetAddresses.asInetSocketAddress;
import static io.trino.plugin.base.galaxy.InetAddresses.toInetAddresses;
import static io.trino.spi.galaxy.CatalogNetworkMonitor.getCatalogNetworkMonitor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class GalaxySqlSocketFactory
        extends SocketFactory
{
    private static final String CATALOG_NAME_PROPERTY_NAME = "catalogName";
    private static final String CATALOG_ID_PROPERTY_NAME = "catalogId";
    private static final String CROSS_REGION_ALLOWED_PROPERTY_NAME = "crossRegionAllowed";
    private static final String REGION_LOCAL_IP_ADDRESSES_PROPERTY_NAME = "regionLocalIpAddresses";
    // tlsEnabled is only needed for the mongo socket factory currently
    // JDBC drivers wrap the socket in an SSLSocket if necessary
    private static final String TLS_ENABLED_PROPERTY_NAME = "tlsEnabled";

    // Use static caches to retain state because socket factories are unfortunately recreated for each socket
    private static final LoadingCache<List<String>, IpRangeMatcher> IP_RANGE_MATCHER_CACHE =
            buildNonEvictableCache(CacheBuilder.newBuilder(), CacheLoader.from(IpRangeMatcher::create));

    private final String catalogName;
    private final String catalogId;
    private final boolean crossRegionAllowed;
    private final IpRangeMatcher ipRangeMatcher;
    private final Optional<SshTunnelManager> sshTunnelManager;
    private final boolean tlsEnabled;

    public GalaxySqlSocketFactory(Properties properties)
    {
        requireNonNull(properties, "properties is null");
        catalogName = getCatalogName(properties);
        catalogId = getCatalogId(properties);
        crossRegionAllowed = isCrossRegionAllowed(properties);
        ipRangeMatcher = IP_RANGE_MATCHER_CACHE.getUnchecked(getRegionLocalIpAddresses(properties));
        sshTunnelManager = getSshTunnelProperties(properties)
                .map(SshTunnelManager::getCached);
        tlsEnabled = getOptionalProperty(properties, TLS_ENABLED_PROPERTY_NAME)
                .map(Boolean::parseBoolean)
                .orElse(false);
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
        private boolean crossRegionAddress;

        public SocketAddress redirectAddress(SocketAddress socketAddress)
                throws IOException
        {
            InetSocketAddress inetSocketAddress = asInetSocketAddress(socketAddress);

            if (sshTunnelManager.isPresent()) {
                SshTunnelManager tunnelManager = sshTunnelManager.get();
                Tunnel tunnel = tunnelManager.getOrCreateTunnel(HostAndPort.fromParts(inetSocketAddress.getHostString(), inetSocketAddress.getPort()));
                // Verify that the SSH server will be within the region-local IP ranges (because it will be our first network hop)
                verifyRegionLocalIps("SSH tunnel server", toInetAddresses(tunnelManager.getSshServer().getHost()));
                return new InetSocketAddress("127.0.0.1", tunnel.getLocalTunnelPort());
            }

            verifyRegionLocalIp("Database server", extractInetAddress(inetSocketAddress));
            return inetSocketAddress;
        }

        public InputStream getInputStream(InputStream inputStream)
                throws IOException
        {
            return getCatalogNetworkMonitor(catalogName, catalogId).monitorInputStream(crossRegionAddress, inputStream);
        }

        public OutputStream getOutputStream(OutputStream outputStream)
                throws IOException
        {
            return getCatalogNetworkMonitor(catalogName, catalogId).monitorOutputStream(crossRegionAddress, outputStream);
        }

        private void verifyRegionLocalIps(String serverType, List<InetAddress> addresses)
                throws IOException
        {
            for (InetAddress inetAddress : addresses) {
                verifyRegionLocalIp(serverType, inetAddress);
            }
        }

        private void verifyRegionLocalIp(String serverType, InetAddress inetAddress)
                throws IOException
        {
            if (!ipRangeMatcher.matches(inetAddress)) {
                if (!crossRegionAllowed) {
                    throw new IOException(format("%s %s is not in an allowed region", serverType, toAddrString(inetAddress)));
                }
                crossRegionAddress = true;
            }
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

    public static void addCrossRegionAllowed(Properties properties, boolean crossRegionAllowed)
    {
        properties.setProperty(CROSS_REGION_ALLOWED_PROPERTY_NAME, Boolean.toString(crossRegionAllowed));
    }

    private static boolean isCrossRegionAllowed(Properties properties)
    {
        return Boolean.parseBoolean(getRequiredProperty(properties, CROSS_REGION_ALLOWED_PROPERTY_NAME));
    }

    public static void addRegionLocalIpAddresses(Properties properties, List<String> regionLocalIpAddresses)
    {
        properties.setProperty(REGION_LOCAL_IP_ADDRESSES_PROPERTY_NAME, Joiner.on(",").join(regionLocalIpAddresses));
    }

    private static List<String> getRegionLocalIpAddresses(Properties properties)
    {
        return Splitter.on(',').splitToList(getRequiredProperty(properties, REGION_LOCAL_IP_ADDRESSES_PROPERTY_NAME));
    }

    public static void addTlsEnabled(Properties properties)
    {
        properties.setProperty(TLS_ENABLED_PROPERTY_NAME, "true");
    }

    private static Optional<SshTunnelProperties> getSshTunnelProperties(Properties properties)
    {
        return SshTunnelPropertiesMapper.getSshTunnelProperties(name -> getOptionalProperty(properties, name));
    }

    private static String getRequiredProperty(Properties properties, String propertyName)
    {
        return getOptionalProperty(properties, propertyName)
                .orElseThrow(() -> new IllegalArgumentException("Missing required property: " + propertyName));
    }

    private static Optional<String> getOptionalProperty(Properties properties, String propertyName)
    {
        return Optional.ofNullable(properties.getProperty(propertyName));
    }

    private static InetAddress extractInetAddress(InetSocketAddress socketAddress)
    {
        checkArgument(!socketAddress.isUnresolved(), "IP address should already be resolved");
        return socketAddress.getAddress();
    }
}
