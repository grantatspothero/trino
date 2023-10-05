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
package io.trino.plugin.cassandra.galaxy;

import com.google.common.net.HostAndPort;
import io.trino.plugin.base.galaxy.RegionVerifier;
import io.trino.sshtunnel.SshTunnelManager;
import io.trino.sshtunnel.SshTunnelManager.Tunnel;
import io.trino.sshtunnel.SshTunnelPropertiesMapper;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.Properties;

import static io.trino.plugin.base.galaxy.InetAddresses.toInetAddresses;
import static io.trino.plugin.base.galaxy.RegionVerifierProperties.getRegionVerifierProperties;
import static java.util.Objects.requireNonNull;

public class GalaxyCassandraTunnelManager
{
    private final Optional<SshTunnelManager> sshTunnelManager;
    private final RegionVerifier regionVerifier;

    public GalaxyCassandraTunnelManager(Properties properties)
    {
        requireNonNull(properties, "properties is null");
        this.sshTunnelManager =
                SshTunnelPropertiesMapper
                        .getSshTunnelProperties(name -> Optional.ofNullable(properties.getProperty(name)))
                        .map(SshTunnelManager::getCached);
        regionVerifier = new RegionVerifier(getRegionVerifierProperties(properties::getProperty));
    }

    public SocketAddress getTunnelAddress(InetSocketAddress inetSocketAddress)
    {
        if (sshTunnelManager.isPresent()) {
            SshTunnelManager tunnelManager = sshTunnelManager.get();
            Tunnel tunnel = tunnelManager.getOrCreateTunnel(HostAndPort.fromParts(inetSocketAddress.getHostString(), inetSocketAddress.getPort()));
            // Verify that the SSH server will be within the region-local IP ranges (because it will be our first network hop)
            regionVerifier.isCrossRegionAccess("SSH tunnel server", toInetAddresses(tunnelManager.getSshServer().getHost()));
            return new InetSocketAddress("127.0.0.1", tunnel.getLocalTunnelPort());
        }
        regionVerifier.isCrossRegionAccess("Cassandra server", inetSocketAddress);
        return inetSocketAddress;
    }
}
