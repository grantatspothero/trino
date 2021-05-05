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
package io.trino.plugin.hive.metastore.galaxy;

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.metastore.client.MetastoreId;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGalaxyHiveMetastoreConfig
{
    private static final String SHARED_SECRET = "1234567890123456789012345678901234567890123456789012345678901234";

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxyHiveMetastoreConfig.class)
                .setMetastoreId(null)
                .setSharedSecret(null)
                .setServerUri(null)
                .setDefaultDataDirectory(null));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.metastore.metastore-id", "ms-1234567890")
                .put("galaxy.metastore.shared-secret", SHARED_SECRET)
                .put("galaxy.metastore.server-uri", "https://example.com")
                .put("galaxy.metastore.default-data-dir", "some path")
                .buildOrThrow();

        GalaxyHiveMetastoreConfig expected = new GalaxyHiveMetastoreConfig()
                .setMetastoreId(new MetastoreId("ms-1234567890"))
                .setSharedSecret(SHARED_SECRET)
                .setServerUri(URI.create("https://example.com"))
                .setDefaultDataDirectory("some path");

        assertFullMapping(properties, expected);
    }
}
