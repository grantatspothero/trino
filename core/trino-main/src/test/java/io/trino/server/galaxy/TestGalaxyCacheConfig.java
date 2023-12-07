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
import io.trino.connector.informationschema.galaxy.GalaxyCacheConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGalaxyCacheConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxyCacheConfig.class)
                .setEnabled(false)
                .setSessionDefaultEnabled(false)
                .setSessionDefaultCatalogsRegex(".*")
                .setSessionDefaultMinimumIndexAge(Duration.valueOf("1m"))
                .setSessionDefaultMaximumIndexAge(Duration.valueOf("1.5d")));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.metadata-cache.enabled", "true")
                .put("galaxy.metadata-cache.session-default.enabled", "true")
                .put("galaxy.metadata-cache.session-default.catalogs-regex", "XX")
                .put("galaxy.metadata-cache.session-default.minimum-age", "5m")
                .put("galaxy.metadata-cache.session-default.maximum-age", "3d")
                .buildOrThrow();

        GalaxyCacheConfig expected = new GalaxyCacheConfig()
                .setEnabled(true)
                .setSessionDefaultEnabled(true)
                .setSessionDefaultCatalogsRegex("XX")
                .setSessionDefaultMinimumIndexAge(Duration.valueOf("5m"))
                .setSessionDefaultMaximumIndexAge(Duration.valueOf("3d"));

        assertFullMapping(properties, expected);
    }
}
