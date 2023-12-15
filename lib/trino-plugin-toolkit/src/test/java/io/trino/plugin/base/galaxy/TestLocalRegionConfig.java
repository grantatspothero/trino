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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestLocalRegionConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LocalRegionConfig.class)
                .setAllowedIpAddresses(ImmutableList.of("0.0.0.0/0")));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.local-region.allowed-ip-addresses", "1.2.3.4, 5.6.7.8/29")
                .buildOrThrow();

        LocalRegionConfig expected = new LocalRegionConfig()
                .setAllowedIpAddresses(ImmutableList.of("1.2.3.4", "5.6.7.8/29"));

        assertFullMapping(properties, expected);
    }
}
