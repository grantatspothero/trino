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
package io.trino.plugin.eventlistener.galaxy;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGalaxyKafkaEventListenerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxyKafkaEventListenerConfig.class)
                .setPluginReportingName(null)
                .setAccountId(null)
                .setClusterId(null)
                .setDeploymentId(null)
                .setTrinoPlaneFqdn(null)
                .setEventKafkaTopic(null)
                .setLifeCycleEventKafkaTopic(null)
                .setMaxBufferingCapacity(200));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("plugin.reporting-name", "test-name")
                .put("galaxy.account-id", "a-123")
                .put("galaxy.cluster-id", "c-234")
                .put("galaxy.deployment-id", "dep-345")
                .put("galaxy.trino-plane-fqdn", "trino.example.com")
                .put("galaxy.event.kafka.topic", "test1")
                .put("galaxy.lifecycle-event.kafka.topic", "test2")
                .put("publisher.max-buffering-capacity", "10")
                .buildOrThrow();

        GalaxyKafkaEventListenerConfig expected = new GalaxyKafkaEventListenerConfig()
                .setPluginReportingName("test-name")
                .setAccountId("a-123")
                .setClusterId("c-234")
                .setDeploymentId("dep-345")
                .setTrinoPlaneFqdn("trino.example.com")
                .setEventKafkaTopic("test1")
                .setLifeCycleEventKafkaTopic("test2")
                .setMaxBufferingCapacity(10);

        assertFullMapping(properties, expected);
    }
}
