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
package io.trino.server.galaxy;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGalaxyHeartbeatConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxyHeartbeatConfig.class)
                .setTrinoPlaneFqdn(null)
                .setVariant(null)
                .setRole(null)
                .setBillingTopic("billing")
                .setPublishInterval(new Duration(30, TimeUnit.SECONDS))
                .setTerminationGracePeriod(new Duration(10, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.trino-plane-fqdn", "gcp-test-1.galaxy.gate0.net")
                .put("galaxy.cluster-variant", "demo")
                .put("galaxy.trino-instance-role", "coordinator")
                .put("galaxy.billing.topic", "notbilling")
                .put("galaxy.heartbeat.publish-interval", "5s")
                .put("galaxy.heartbeat.termination-grace-period", "5m")
                .buildOrThrow();

        GalaxyHeartbeatConfig expected = new GalaxyHeartbeatConfig()
                .setTrinoPlaneFqdn("gcp-test-1.galaxy.gate0.net")
                .setVariant("demo")
                .setRole("coordinator")
                .setBillingTopic("notbilling")
                .setPublishInterval(new Duration(5, TimeUnit.SECONDS))
                .setTerminationGracePeriod(new Duration(5, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }
}