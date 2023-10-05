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

import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static io.trino.plugin.base.galaxy.InetAddresses.asInetSocketAddress;
import static java.util.Objects.requireNonNull;

public class GalaxyNioSocketChannel
        extends NioSocketChannel
{
    private GalaxyCassandraTunnelManager galaxyCassandraTunnelManager;

    public GalaxyNioSocketChannel()
    {
        super();
    }

    public void setGalaxyCassandraTunnelManager(GalaxyCassandraTunnelManager galaxyCassandraTunnelManager)
    {
        this.galaxyCassandraTunnelManager = requireNonNull(galaxyCassandraTunnelManager, "galaxyCassandraTunnelManager is null");
    }

    @Override
    protected boolean doConnect(SocketAddress remoteAddress, SocketAddress localAddress)
            throws Exception
    {
        // Background - Cassandra drivers perform node discovery and tries to connect to those in case all the nodes are not given in the input. When an attempt is made to communicate with a node discoverable through private ip will lead to a node unreachable failure warnings.
        // Region should not be enforced and tunnel should not be created against private ip when driver tries to connect to a node through private ip.
        InetSocketAddress inetRemoteSocketAddress = asInetSocketAddress(remoteAddress);
        if (!inetRemoteSocketAddress.getAddress().isSiteLocalAddress()) {
            remoteAddress = galaxyCassandraTunnelManager.getTunnelAddress(inetRemoteSocketAddress);
        }
        return super.doConnect(remoteAddress, localAddress);
    }
}
