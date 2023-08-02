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
package io.trino.plugin.base.galaxy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestRegionEnforcementConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RegionEnforcementConfig.class)
                .setAllowCrossRegionAccess(false)
                .setCrossRegionReadLimit(DataSize.of(20, Unit.GIGABYTE))
                .setCrossRegionWriteLimit(DataSize.of(10, Unit.GIGABYTE))
                .setAllowedIpAddresses(ImmutableList.of("0.0.0.0/0")));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.region-enforcement.allow-cross-region-access", "true")
                .put("galaxy.region-enforcement.cross-region-read-limit", "500TB")
                .put("galaxy.region-enforcement.cross-region-write-limit", "250MB")
                .put("galaxy.region-enforcement.allowed-ip-addresses", "1.2.3.4, 5.6.7.8/29")
                .buildOrThrow();

        RegionEnforcementConfig expected = new RegionEnforcementConfig()
                .setAllowCrossRegionAccess(true)
                .setCrossRegionReadLimit(DataSize.of(500, Unit.TERABYTE))
                .setCrossRegionWriteLimit(DataSize.of(250, Unit.MEGABYTE))
                .setAllowedIpAddresses(ImmutableList.of("1.2.3.4", "5.6.7.8/29"));

        assertFullMapping(properties, expected);
    }
}
