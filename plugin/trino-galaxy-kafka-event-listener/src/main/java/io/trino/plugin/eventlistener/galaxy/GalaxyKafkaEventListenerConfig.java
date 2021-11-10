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
package io.trino.plugin.eventlistener.galaxy;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Positive;

public class GalaxyKafkaEventListenerConfig
{
    private String pluginReportingName;
    private String accountId;
    private String clusterId;
    private String deploymentId;
    private String trinoPlaneFqdn;
    private String eventKafkaTopic;
    private int maxBufferingCapacity = 200;

    @NotEmpty
    public String getPluginReportingName()
    {
        return pluginReportingName;
    }

    @Config("plugin.reporting-name")
    @ConfigDescription("Name to distinguish this plugin instance from other instances of the same type in logs and stats")
    public GalaxyKafkaEventListenerConfig setPluginReportingName(String pluginReportingName)
    {
        this.pluginReportingName = pluginReportingName;
        return this;
    }

    @NotEmpty
    public String getAccountId()
    {
        return accountId;
    }

    @Config("galaxy.account-id")
    public GalaxyKafkaEventListenerConfig setAccountId(String accountId)
    {
        this.accountId = accountId;
        return this;
    }

    @NotEmpty
    public String getClusterId()
    {
        return clusterId;
    }

    @Config("galaxy.cluster-id")
    public GalaxyKafkaEventListenerConfig setClusterId(String clusterId)
    {
        this.clusterId = clusterId;
        return this;
    }

    @NotEmpty
    public String getDeploymentId()
    {
        return deploymentId;
    }

    @Config("galaxy.deployment-id")
    public GalaxyKafkaEventListenerConfig setDeploymentId(String deploymentId)
    {
        this.deploymentId = deploymentId;
        return this;
    }

    @NotEmpty
    public String getTrinoPlaneFqdn()
    {
        return trinoPlaneFqdn;
    }

    @Config("galaxy.trino-plane-fqdn")
    public GalaxyKafkaEventListenerConfig setTrinoPlaneFqdn(String trinoPlaneFqdn)
    {
        this.trinoPlaneFqdn = trinoPlaneFqdn;
        return this;
    }

    @NotEmpty
    public String getEventKafkaTopic()
    {
        return eventKafkaTopic;
    }

    @Config("galaxy.event.kafka.topic")
    @ConfigDescription("Kafka topic to publish events")
    public GalaxyKafkaEventListenerConfig setEventKafkaTopic(String eventKafkaTopic)
    {
        this.eventKafkaTopic = eventKafkaTopic;
        return this;
    }

    @Positive
    public int getMaxBufferingCapacity()
    {
        return maxBufferingCapacity;
    }

    @Config("publisher.max-buffering-capacity")
    @ConfigDescription("Maximum number of records that the publisher will retain before starting to drop")
    public GalaxyKafkaEventListenerConfig setMaxBufferingCapacity(int maxBufferingCapacity)
    {
        this.maxBufferingCapacity = maxBufferingCapacity;
        return this;
    }
}
