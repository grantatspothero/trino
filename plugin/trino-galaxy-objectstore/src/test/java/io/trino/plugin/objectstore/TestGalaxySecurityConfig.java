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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.id.CatalogId;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGalaxySecurityConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxySecurityConfig.class)
                .setEnabled(true)
                .setAccountUri(null)
                .setAccessControlUri(null)
                .setCatalogId(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.location-security.enabled", "false")
                .put("galaxy.account-url", "https://whackadoodle.galaxy.com")
                .put("galaxy.access-control-url", "https://whackadoodle.aws-us-east1.accesscontrol.galaxy.com")
                .put("galaxy.catalog-id", "c-1234567890")
                .buildOrThrow();

        GalaxySecurityConfig expected = new GalaxySecurityConfig()
                .setEnabled(false)
                .setAccountUri(URI.create("https://whackadoodle.galaxy.com"))
                .setAccessControlUri(URI.create("https://whackadoodle.aws-us-east1.accesscontrol.galaxy.com"))
                .setCatalogId(new CatalogId("c-1234567890"));

        assertFullMapping(properties, expected);
    }
}