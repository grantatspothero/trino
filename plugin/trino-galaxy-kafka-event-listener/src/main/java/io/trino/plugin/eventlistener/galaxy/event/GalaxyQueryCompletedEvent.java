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
package io.trino.plugin.eventlistener.galaxy.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.eventlistener.QueryCompletedEvent;

import static java.util.Objects.requireNonNull;

public class GalaxyQueryCompletedEvent
{
    private final String accountId;
    private final String clusterId;
    private final String deploymentId;
    private final QueryCompletedEvent event;

    @JsonCreator
    public GalaxyQueryCompletedEvent(String accountId, String clusterId, String deploymentId, QueryCompletedEvent event)
    {
        this.accountId = requireNonNull(accountId, "accountId is null");
        this.clusterId = requireNonNull(clusterId, "clusterId is null");
        this.deploymentId = requireNonNull(deploymentId, "deploymentId is null");
        this.event = requireNonNull(event, "event is null");
    }

    @JsonProperty
    public String getAccountId()
    {
        return accountId;
    }

    @JsonProperty
    public String getClusterId()
    {
        return clusterId;
    }

    @JsonProperty
    public String getDeploymentId()
    {
        return deploymentId;
    }

    @JsonProperty
    public QueryCompletedEvent getEvent()
    {
        return event;
    }
}
