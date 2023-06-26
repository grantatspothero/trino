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
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class GalaxyHeartbeatConfig
{
    private String variant;
    private String role;
    private String trinoPlaneFqdn;
    private String billingTopic = "billing";
    private Duration publishInterval = new Duration(30, TimeUnit.SECONDS);
    private Duration terminationGracePeriod = new Duration(10, TimeUnit.SECONDS);

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

    @NotNull
    public Duration getPublishInterval()
    {
        return publishInterval;
    }

    @Config("galaxy.heartbeat.publish-interval")
    @ConfigDescription("The interval between published heartbeat events")
    public GalaxyHeartbeatConfig setPublishInterval(Duration publishInterval)
    {
        this.publishInterval = publishInterval;
        return this;
    }

    @NotNull
    public Duration getTerminationGracePeriod()
    {
        return terminationGracePeriod;
    }

    @Config("galaxy.heartbeat.termination-grace-period")
    @ConfigDescription("The termination grace period for publishing a final heartbeat when the heartbeat publisher is shut down")
    public GalaxyHeartbeatConfig setTerminationGracePeriod(Duration terminationGracePeriod)
    {
        this.terminationGracePeriod = terminationGracePeriod;
        return this;
    }
}
