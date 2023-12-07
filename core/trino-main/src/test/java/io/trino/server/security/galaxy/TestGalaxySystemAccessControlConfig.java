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
import io.trino.server.security.galaxy.GalaxySystemAccessControlConfig.FilterColumnsAcceleration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestGalaxySystemAccessControlConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxySystemAccessControlConfig.class)
                .setFilterColumnsAcceleration(FilterColumnsAcceleration.NONE)
                .setBackgroundProcessingThreads(8)
                .setExpectedQueryParallelism(100));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.filter-columns-acceleration", "FCX2")
                .put("galaxy.access-control-background-threads", "3")
                .put("galaxy.expected-query-parallelism", "200")
                .buildOrThrow();

        GalaxySystemAccessControlConfig expected = new GalaxySystemAccessControlConfig()
                .setFilterColumnsAcceleration(FilterColumnsAcceleration.FCX2)
                .setBackgroundProcessingThreads(3)
                .setExpectedQueryParallelism(200);

        assertFullMapping(properties, expected);
    }
}
