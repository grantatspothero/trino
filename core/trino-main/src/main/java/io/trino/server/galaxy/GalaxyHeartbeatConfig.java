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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotEmpty;

public class GalaxyHeartbeatConfig
{
    private String variant;
    private String role;
    private String trinoPlaneFqdn;
    private String billingTopic = "billing";

    @NotEmpty
    public String getVariant()
    {
        return variant;
    }

    @Config("galaxy.cluster-variant")
    public GalaxyHeartbeatConfig setVariant(String variant)
    {
        this.variant = variant;
        return this;
    }

    @NotEmpty
    public String getRole()
    {
        return role;
    }

    @Config("galaxy.trino-instance-role")
    public GalaxyHeartbeatConfig setRole(String role)
    {
        this.role = role;
        return this;
    }

    @NotEmpty
    public String getTrinoPlaneFqdn()
    {
        return trinoPlaneFqdn;
    }

    @Config("galaxy.trino-plane-fqdn")
    public GalaxyHeartbeatConfig setTrinoPlaneFqdn(String trinoPlaneFqdn)
    {
        this.trinoPlaneFqdn = trinoPlaneFqdn;
        return this;
    }

    @NotEmpty
    public String getBillingTopic()
    {
        return billingTopic;
    }

    @Config("galaxy.billing.topic")
    @ConfigDescription("The kafka topic name to use for billing events")
    public GalaxyHeartbeatConfig setBillingTopic(String billingTopic)
    {
        this.billingTopic = billingTopic;
        return this;
    }
}
