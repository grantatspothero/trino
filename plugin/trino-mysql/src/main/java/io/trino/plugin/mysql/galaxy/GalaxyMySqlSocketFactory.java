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
package io.trino.plugin.mysql.galaxy;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.net.HostAndPort;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.protocol.StandardSocketFactory;
import io.trino.plugin.base.galaxy.IpRangeMatcher;
import io.trino.sshtunnel.SshTunnelManager;
import io.trino.sshtunnel.SshTunnelManager.Tunnel;
import io.trino.sshtunnel.SshTunnelProperties;
import io.trino.sshtunnel.SshTunnelPropertiesMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
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

public class GalaxyMySqlSocketFactory
        extends StandardSocketFactory
{
    private static final String CATALOG_NAME_PROPERTY_NAME = "catalogName";
    private static final String CATALOG_ID_PROPERTY_NAME = "catalogId";
    private static final String CROSS_REGION_ALLOWED_PROPERTY_NAME = "crossRegionAllowed";
    private static final String REGION_LOCAL_IP_ADDRESSES_PROPERTY_NAME = "regionLocalIpAddresses";

    // Use static caches to retain state because socket factories are unfortunately recreated for each socket
    private static final LoadingCache<List<String>, IpRangeMatcher> IP_RANGE_MATCHER_CACHE =
            buildNonEvictableCache(CacheBuilder.newBuilder(), CacheLoader.from(IpRangeMatcher::create));

    @Override
    protected Socket createSocket(PropertySet props)
    {
        String catalogName = getCatalogName(props);
        String catalogId = getCatalogId(props);
        boolean crossRegionAllowed = isCrossRegionAllowed(props);
        IpRangeMatcher ipRangeMatcher = IP_RANGE_MATCHER_CACHE.getUnchecked(getRegionLocalIpAddresses(props));
        Optional<SshTunnelManager> sshTunnelManager = getSshTunnelProperties(props)
                .map(SshTunnelManager::getCached);

        return new Socket()
        {
            private boolean crossRegionAddress;

            @Override
            public void connect(SocketAddress endpoint)
                    throws IOException
            {
                super.connect(process(endpoint));
            }

            @Override
            public void connect(SocketAddress endpoint, int timeout)
                    throws IOException
            {
                super.connect(process(endpoint), timeout);
            }

            private SocketAddress process(SocketAddress socketAddress)
                    throws IOException
            {
                InetSocketAddress inetSocketAddress = asInetSocketAddress(socketAddress);

                if (sshTunnelManager.isPresent()) {
                    SshTunnelManager tunnelManager = sshTunnelManager.get();
                    Tunnel tunnel = tunnelManager.getOrCreateTunnel(HostAndPort.fromParts(inetSocketAddress.getHostString(), inetSocketAddress.getPort()));
                    // Verify that the SSH server will be in the allowed IP ranges (because it will be our first network hop)
                    verifyRegionLocalIps("SSH tunnel server", toInetAddresses(tunnelManager.getSshServer().getHost()));
                    return new InetSocketAddress("127.0.0.1", tunnel.getLocalTunnelPort());
                }

                verifyRegionLocalIp("MySQL server", extractInetAddress(inetSocketAddress));
                return inetSocketAddress;
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

            @Override
            public InputStream getInputStream()
                    throws IOException
            {
                return getCatalogNetworkMonitor(catalogName, catalogId).monitorInputStream(crossRegionAddress, super.getInputStream());
            }

            @Override
            public OutputStream getOutputStream()
                    throws IOException
            {
                return getCatalogNetworkMonitor(catalogName, catalogId).monitorOutputStream(crossRegionAddress, super.getOutputStream());
            }
        };
    }

    public static void addCatalogName(Properties properties, String catalogName)
    {
        properties.setProperty(CATALOG_NAME_PROPERTY_NAME, catalogName);
    }

    private static String getCatalogName(PropertySet properties)
    {
        return getRequiredProperty(properties, CATALOG_NAME_PROPERTY_NAME);
    }

    public static void addCatalogId(Properties properties, String catalogId)
    {
        properties.setProperty(CATALOG_ID_PROPERTY_NAME, catalogId);
    }

    private static String getCatalogId(PropertySet properties)
    {
        return getRequiredProperty(properties, CATALOG_ID_PROPERTY_NAME);
    }

    public static void addCrossRegionAllowed(Properties properties, boolean crossRegionAllowed)
    {
        properties.setProperty(CROSS_REGION_ALLOWED_PROPERTY_NAME, Boolean.toString(crossRegionAllowed));
    }

    private static boolean isCrossRegionAllowed(PropertySet properties)
    {
        return Boolean.parseBoolean(getRequiredProperty(properties, CROSS_REGION_ALLOWED_PROPERTY_NAME));
    }

    public static void addRegionLocalIpAddresses(Properties properties, List<String> regionLocalIpAddresses)
    {
        properties.setProperty(REGION_LOCAL_IP_ADDRESSES_PROPERTY_NAME, Joiner.on(",").join(regionLocalIpAddresses));
    }

    private static List<String> getRegionLocalIpAddresses(PropertySet propertySet)
    {
        return Splitter.on(',').splitToList(getRequiredProperty(propertySet, REGION_LOCAL_IP_ADDRESSES_PROPERTY_NAME));
    }

    public static void addSshTunnelProperties(Properties properties, SshTunnelProperties sshTunnelProperties)
    {
        SshTunnelPropertiesMapper.addSshTunnelProperties(properties::setProperty, sshTunnelProperties);
    }

    private static Optional<SshTunnelProperties> getSshTunnelProperties(PropertySet propertySet)
    {
        return SshTunnelPropertiesMapper.getSshTunnelProperties(name -> getOptionalProperty(propertySet, name));
    }

    private static String getRequiredProperty(PropertySet properties, String propertyName)
    {
        return getOptionalProperty(properties, propertyName)
                .orElseThrow(() -> new IllegalArgumentException("Missing required property: " + propertyName));
    }

    private static Optional<String> getOptionalProperty(PropertySet properties, String propertyName)
    {
        return Optional.ofNullable(properties.getStringProperty(propertyName))
                .map(RuntimeProperty::getStringValue);
    }

    private static InetAddress extractInetAddress(InetSocketAddress socketAddress)
    {
        checkArgument(!socketAddress.isUnresolved(), "StandardSocketFactory should have already resolved the IP address");
        return socketAddress.getAddress();
    }
}
