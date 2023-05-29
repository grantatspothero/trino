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
package io.trino.plugin.pinot.galaxy;

import com.google.common.net.HostAndPort;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.trino.plugin.base.galaxy.RegionEnforcementConfig;
import io.trino.plugin.base.galaxy.RegionVerifier;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelManager;
import io.trino.sshtunnel.SshTunnelProperties;

import javax.inject.Inject;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

public class GalaxyFilter
        implements HttpRequestFilter
{
    private final RegionVerifier regionVerifier;
    private final Optional<SshTunnelManager> sshTunnelManager;

    @Inject
    public GalaxyFilter(RegionEnforcementConfig regionEnforcementConfig, SshTunnelConfig sshTunnelConfig)
    {
        verify(!regionEnforcementConfig.getAllowCrossRegionAccess(), "Cross-region access not supported");
        this.regionVerifier = new RegionVerifier(regionEnforcementConfig.getAllowCrossRegionAccess(), regionEnforcementConfig.getAllowedIpAddresses());
        this.sshTunnelManager = SshTunnelProperties.generateFrom(sshTunnelConfig).map(SshTunnelManager::getCached);
    }

    @Override
    public Request filterRequest(Request request)
    {
        if (sshTunnelManager.isPresent()) {
            regionVerifier.verifyLocalRegion("SSH tunnel server", sshTunnelManager.get().getSshServer().getHost());
            SshTunnelManager.Tunnel tunnel = sshTunnelManager.get().getOrCreateTunnel(HostAndPort.fromParts(request.getUri().getHost(), request.getUri().getPort()));
            return Request.Builder.fromRequest(request)
                    .setUri(uriBuilderFrom(request.getUri()).host("127.0.0.1").port(tunnel.getLocalTunnelPort()).build())
                    .build();
        }
        regionVerifier.verifyLocalRegion("Pinot server", request.getUri().getHost());
        return request;
    }
}
