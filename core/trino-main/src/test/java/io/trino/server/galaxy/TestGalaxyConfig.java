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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGalaxyConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxyConfig.class)
                .setAccountId(null)
                .setClusterId(null)
                .setDeploymentId(null)
                .setCloudRegionId(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.account-id", "a-123456")
                .put("galaxy.cluster-id", "c-123654")
                .put("galaxy.deployment-id", "d-123567")
                .put("galaxy.cloud-region-id", "aws-eu-west1")
                .buildOrThrow();

        GalaxyConfig expected = new GalaxyConfig()
                .setAccountId("a-123456")
                .setClusterId("c-123654")
                .setDeploymentId("d-123567")
                .setCloudRegionId("aws-eu-west1");

        assertFullMapping(properties, expected);
    }
}