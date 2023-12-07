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
package io.trino.server.security.galaxy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.id.CatalogId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGalaxyAccessControlConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxyAccessControlConfig.class)
                // No defaults for the accountUri
                .setAccountUri(null)
                .setAccessControlOverrideUri(null)
                .setCatalogNames("")
                .setReadOnlyCatalogs(""));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.account-url", "https://whackadoodle.galaxy.com")
                .put("galaxy.access-control-url", "https://whackadoodle.aws-us-east1.accesscontrol.galaxy.com")
                .put("galaxy.catalog-names", "my_catalog->c-1234567890,other_catalog->c-1112223334")
                .put("galaxy.read-only-catalogs", "sillycatalog,funnycatalog")
                .buildOrThrow();

        GalaxyAccessControlConfig expected = new GalaxyAccessControlConfig()
                .setAccountUri(URI.create("https://whackadoodle.galaxy.com"))
                .setAccessControlOverrideUri(URI.create("https://whackadoodle.aws-us-east1.accesscontrol.galaxy.com"))
                .setCatalogNames(ImmutableMap.<String, CatalogId>builder()
                        .put("my_catalog", new CatalogId("c-1234567890"))
                        .put("other_catalog", new CatalogId("c-1112223334"))
                        .buildOrThrow())
                .setReadOnlyCatalogs(ImmutableSet.of("sillycatalog", "funnycatalog"));

        assertFullMapping(properties, expected);
    }
}
