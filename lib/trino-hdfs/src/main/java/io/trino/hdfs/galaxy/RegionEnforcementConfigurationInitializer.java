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
package io.trino.hdfs.galaxy;

import io.trino.hdfs.ConfigurationInitializer;
import io.trino.plugin.base.galaxy.CidrBlock;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class RegionEnforcementConfigurationInitializer
        implements ConfigurationInitializer
{
    private final boolean enabled;
    private final Optional<String> allowedS3Region;
    private final Optional<String> allowedGcpRegion;
    private final Optional<List<String>> allowedAzureIpAddresses;

    @Inject
    public RegionEnforcementConfigurationInitializer(RegionEnforcementConfig config)
    {
        enabled = config.isEnabled();
        allowedS3Region = config.getAllowedS3Region();
        allowedGcpRegion = config.getAllowedGcpRegion();

        // ensure that the ip addresses are valid
        config.getAllowedAzureIpAddresses().stream().flatMap(Collection::stream).forEach(CidrBlock::new);
        allowedAzureIpAddresses = config.getAllowedAzureIpAddresses();
    }

    @Override
    public void initializeConfiguration(Configuration config)
    {
        RegionEnforcement.addEnforceRegion(config, enabled);
        if (enabled) {
            allowedS3Region.ifPresent(region -> RegionEnforcement.addS3AllowedRegion(config, region));
            allowedGcpRegion.ifPresent(region -> RegionEnforcement.addGcsAllowedRegion(config, region));
            allowedAzureIpAddresses.ifPresent(ipAddresses -> RegionEnforcement.addAzureAllowedIpAddresses(config, ipAddresses));
        }
    }
}
