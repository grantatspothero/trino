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
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.RegionVerifier;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.spi.galaxy.CatalogConnectionType;
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

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.galaxy.InetAddresses.asInetSocketAddress;
import static io.trino.plugin.base.galaxy.InetAddresses.toInetAddresses;
import static io.trino.spi.galaxy.CatalogNetworkMonitor.getCatalogNetworkMonitor;
import static io.trino.spi.galaxy.CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor;

public class GalaxyMySqlSocketFactory
        extends StandardSocketFactory
{
    @Override
    protected Socket createSocket(PropertySet props)
    {
        RegionVerifier regionVerifier = new RegionVerifier(RegionVerifierProperties.getRegionVerifierProperties(propertyName -> getRequiredProperty(props, propertyName)));
        Optional<SshTunnelManager> sshTunnelManager = getSshTunnelProperties(props)
                .map(SshTunnelManager::getCached);
        CatalogNetworkMonitorProperties catalogNetworkMonitorProperties = CatalogNetworkMonitorProperties.getCatalogNetworkMonitorProperties(propertyName -> getOptionalProperty(props, propertyName));
        String catalogName = catalogNetworkMonitorProperties.catalogName();
        String catalogId = catalogNetworkMonitorProperties.catalogId();
        boolean crossRegionAllowed = catalogNetworkMonitorProperties.crossRegionAllowed();
        Optional<DataSize> crossRegionReadLimit = catalogNetworkMonitorProperties.crossRegionReadLimit();
        Optional<DataSize> crossRegionWriteLimit = catalogNetworkMonitorProperties.crossRegionWriteLimit();

        return new Socket()
        {
            private CatalogConnectionType catalogConnectionType;

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
                    catalogConnectionType = regionVerifier.getCatalogConnectionType("SSH tunnel server", toInetAddresses(tunnelManager.getSshServer().getHost()));
                    return new InetSocketAddress("127.0.0.1", tunnel.getLocalTunnelPort());
                }

                catalogConnectionType = regionVerifier.getCatalogConnectionType("Database server", inetSocketAddress);
                return inetSocketAddress;
            }

            @Override
            public InputStream getInputStream()
                    throws IOException
            {
                if (crossRegionAllowed) {
                    verify(crossRegionReadLimit.isPresent(), "Cross-region read limit must be present to query cross-region catalog");
                    verify(crossRegionWriteLimit.isPresent(), "Cross-region write limit must be present to query cross-region catalog");

                    return getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, crossRegionReadLimit.get().toBytes(), crossRegionWriteLimit.get().toBytes()).monitorInputStream(catalogConnectionType, super.getInputStream());
                }
                return getCatalogNetworkMonitor(catalogName, catalogId).monitorInputStream(catalogConnectionType, super.getInputStream());
            }

            @Override
            public OutputStream getOutputStream()
                    throws IOException
            {
                if (crossRegionAllowed) {
                    verify(crossRegionReadLimit.isPresent(), "Cross-region read limit must be present to query cross-region catalog");
                    verify(crossRegionWriteLimit.isPresent(), "Cross-region write limit must be present to query cross-region catalog");

                    return getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, crossRegionReadLimit.get().toBytes(), crossRegionWriteLimit.get().toBytes()).monitorOutputStream(catalogConnectionType, super.getOutputStream());
                }
                return getCatalogNetworkMonitor(catalogName, catalogId).monitorOutputStream(catalogConnectionType, super.getOutputStream());
            }
        };
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
