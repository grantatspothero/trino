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
package io.trino.sshtunnel;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSshTunnelConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SshTunnelConfig.class)
                .setServer(null)
                .setUser(null)
                .setPrivateKey(null)
                .setReconnectCheckInterval(new Duration(2, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ssh-tunnel.server", "example.com:3306")
                .put("ssh-tunnel.user", "user1")
                .put("ssh-tunnel.private-key", "key contents")
                .put("ssh-tunnel.reconnect-check-interval", "5m")
                .buildOrThrow();

        SshTunnelConfig expected = new SshTunnelConfig()
                .setServer(HostAndPort.fromParts("example.com", 3306))
                .setUser("user1")
                .setPrivateKey("key contents")
                .setReconnectCheckInterval(new Duration(5, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }
}
