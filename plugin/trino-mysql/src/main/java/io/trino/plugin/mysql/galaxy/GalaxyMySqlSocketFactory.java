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

import com.google.common.net.HostAndPort;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.protocol.StandardSocketFactory;
import io.airlift.units.DataSize;
import io.trino.plugin.base.galaxy.RegionVerifier;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.sshtunnel.SshTunnelManager;
import io.trino.sshtunnel.SshTunnelManager.Tunnel;
import io.trino.sshtunnel.SshTunnelProperties;
import io.trino.sshtunnel.SshTunnelPropertiesMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.Properties;

import static io.trino.plugin.base.galaxy.InetAddresses.asInetSocketAddress;
import static io.trino.plugin.base.galaxy.InetAddresses.toInetAddresses;
import static io.trino.spi.galaxy.CatalogNetworkMonitor.checkCrossRegionLimitsAndThrowIfExceeded;
import static io.trino.spi.galaxy.CatalogNetworkMonitor.getCatalogNetworkMonitor;

public class GalaxyMySqlSocketFactory
        extends StandardSocketFactory
{
    private static final String CATALOG_NAME_PROPERTY_NAME = "catalogName";
    private static final String CATALOG_ID_PROPERTY_NAME = "catalogId";
    private static final String CROSS_REGION_READ_LIMIT_PROPERTY_NAME = "crossRegionReadLimit";
    private static final String CROSS_REGION_WRITE_LIMIT_PROPERTY_NAME = "crossRegionWriteLimit";

    @Override
    protected Socket createSocket(PropertySet props)
    {
        String catalogName = getCatalogName(props);
        String catalogId = getCatalogId(props);
        RegionVerifier regionVerifier = new RegionVerifier(RegionVerifierProperties.getRegionVerifierProperties(propertyName -> getRequiredProperty(props, propertyName)));
        Optional<DataSize> crossRegionReadLimit = getCrossRegionReadLimit(props);
        Optional<DataSize> crossRegionWriteLimit = getCrossRegionWriteLimit(props);
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
            {
                InetSocketAddress inetSocketAddress = asInetSocketAddress(socketAddress);

                if (sshTunnelManager.isPresent()) {
                    SshTunnelManager tunnelManager = sshTunnelManager.get();
                    Tunnel tunnel = tunnelManager.getOrCreateTunnel(HostAndPort.fromParts(inetSocketAddress.getHostString(), inetSocketAddress.getPort()));
                    // Verify that the SSH server will be in the allowed IP ranges (because it will be our first network hop)
                    crossRegionAddress = regionVerifier.isCrossRegionAccess("SSH tunnel server", toInetAddresses(tunnelManager.getSshServer().getHost()));
                    return new InetSocketAddress("127.0.0.1", tunnel.getLocalTunnelPort());
                }

                crossRegionAddress = regionVerifier.isCrossRegionAccess("Database server", inetSocketAddress);
                return inetSocketAddress;
            }

            @Override
            public InputStream getInputStream()
                    throws IOException
            {
                long crossRegionReadLimitBytes = crossRegionReadLimit.orElseGet(() -> DataSize.ofBytes(0)).toBytes();
                long crossRegionWriteLimitBytes = crossRegionWriteLimit.orElseGet(() -> DataSize.ofBytes(0)).toBytes();

                if (crossRegionAddress) {
                    checkCrossRegionLimitsAndThrowIfExceeded(crossRegionReadLimitBytes, crossRegionWriteLimitBytes);
                }
                return getCatalogNetworkMonitor(catalogName, catalogId, crossRegionReadLimitBytes, crossRegionWriteLimitBytes).monitorInputStream(crossRegionAddress, super.getInputStream());
            }

            @Override
            public OutputStream getOutputStream()
                    throws IOException
            {
                long crossRegionReadLimitBytes = crossRegionReadLimit.orElseGet(() -> DataSize.ofBytes(0)).toBytes();
                long crossRegionWriteLimitBytes = crossRegionWriteLimit.orElseGet(() -> DataSize.ofBytes(0)).toBytes();

                if (crossRegionAddress) {
                    checkCrossRegionLimitsAndThrowIfExceeded(crossRegionReadLimitBytes, crossRegionWriteLimitBytes);
                }
                return getCatalogNetworkMonitor(catalogName, catalogId, crossRegionReadLimitBytes, crossRegionWriteLimitBytes).monitorOutputStream(crossRegionAddress, super.getOutputStream());
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

    public static void addCrossRegionReadLimit(Properties properties, DataSize crossRegionReadLimit)
    {
        properties.setProperty(CROSS_REGION_READ_LIMIT_PROPERTY_NAME, crossRegionReadLimit.toBytesValueString());
    }

    public static void addCrossRegionWriteLimit(Properties properties, DataSize crossRegionWriteLimit)
    {
        properties.setProperty(CROSS_REGION_WRITE_LIMIT_PROPERTY_NAME, crossRegionWriteLimit.toBytesValueString());
    }

    private static Optional<DataSize> getCrossRegionReadLimit(PropertySet propertySet)
    {
        return getOptionalProperty(propertySet, CROSS_REGION_READ_LIMIT_PROPERTY_NAME).map(DataSize::valueOf);
    }

    private static Optional<DataSize> getCrossRegionWriteLimit(PropertySet propertySet)
    {
        return getOptionalProperty(propertySet, CROSS_REGION_WRITE_LIMIT_PROPERTY_NAME).map(DataSize::valueOf);
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
}
