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

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;

public class RegionEnforcementConfig
{
    private boolean enabled;
    private Optional<String> allowedS3Region = Optional.empty();
    private Optional<String> allowedGcpRegion = Optional.empty();
    private Optional<List<String>> allowedAzureIpAddresses = Optional.empty();

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("galaxy.region-enforcement.enabled")
    public RegionEnforcementConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    public Optional<String> getAllowedS3Region()
    {
        return allowedS3Region;
    }

    @Config("galaxy.region-enforcement.s3.allowed-region")
    public RegionEnforcementConfig setAllowedS3Region(String allowedS3Region)
    {
        this.allowedS3Region = Optional.ofNullable(allowedS3Region);
        return this;
    }

    @NotNull
    public Optional<String> getAllowedGcpRegion()
    {
        return allowedGcpRegion;
    }

    @Config("galaxy.region-enforcement.gcp.allowed-region")
    public RegionEnforcementConfig setAllowedGcpRegion(String allowedGcpRegion)
    {
        this.allowedGcpRegion = Optional.ofNullable(allowedGcpRegion);
        return this;
    }

    @NotNull
    public Optional<List<String>> getAllowedAzureIpAddresses()
    {
        return allowedAzureIpAddresses;
    }

    @Config("galaxy.region-enforcement.azure.allowed-ip-addresses")
    public RegionEnforcementConfig setAllowedAzureIpAddresses(List<String> allowedAzureIpAddresses)
    {
        this.allowedAzureIpAddresses = Optional.ofNullable(allowedAzureIpAddresses).map(ImmutableList::copyOf);
        return this;
    }
}
