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
package io.trino.plugin.elasticsearch.client.galaxy;

import com.google.common.net.HostAndPort;
import io.trino.sshtunnel.SshTunnelManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.nio.reactor.SessionRequest;
import org.apache.http.nio.reactor.SessionRequestCallback;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static java.util.Objects.requireNonNull;

public class SshTunnelConnectingIOReactor
        extends DefaultConnectingIOReactor
{
    private final SshTunnelManager sshTunnelManager;

    public SshTunnelConnectingIOReactor(IOReactorConfig config, SshTunnelManager sshTunnelManager)
            throws IOReactorException
    {
        super(config);
        this.sshTunnelManager = requireNonNull(sshTunnelManager, "sshTunnelManager is null");
    }

    @Override
    public SessionRequest connect(SocketAddress remoteAddress, SocketAddress localAddress, Object attachment, SessionRequestCallback callback)
    {
        InetSocketAddress address = (InetSocketAddress) remoteAddress;
        SshTunnelManager.Tunnel tunnel = sshTunnelManager.getOrCreateTunnel(HostAndPort.fromParts(address.getHostString(), address.getPort()));
        // Replace remoteAddress with the new tunnel port when creating a new connection
        SocketAddress tunnelRemoteAddress = new InetSocketAddress("127.0.0.1", tunnel.getLocalTunnelPort());
        return super.connect(tunnelRemoteAddress, localAddress, attachment, callback);
    }
}
