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
import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class LocalRegionConfig
{
    private List<String> allowedIpAddresses = ImmutableList.of("0.0.0.0/0");

    @NotNull
    public List<String> getAllowedIpAddresses()
    {
        return allowedIpAddresses;
    }

    @Config("galaxy.local-region.allowed-ip-addresses")
    public LocalRegionConfig setAllowedIpAddresses(List<String> allowedIpAddresses)
    {
        this.allowedIpAddresses = ImmutableList.copyOf(allowedIpAddresses);
        return this;
    }
}