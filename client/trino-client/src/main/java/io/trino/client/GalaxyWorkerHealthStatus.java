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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class GalaxyWorkerHealthStatus
{
    private final String nodeId;
    private final boolean registeredInDiscovery;

    /**
     * Combined status of all aspects of worker health
     */
    private final boolean isHealthy;

    @JsonCreator
    public GalaxyWorkerHealthStatus(
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("registeredInDiscovery") boolean registeredInDiscovery)
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.registeredInDiscovery = registeredInDiscovery;
        this.isHealthy = registeredInDiscovery;
    }

    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }

    @JsonProperty
    public boolean registeredInDiscovery()
    {
        return registeredInDiscovery;
    }

    @JsonProperty
    public boolean isHealthy()
    {
        return isHealthy;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeId", nodeId)
                .add("registeredInDiscovery", registeredInDiscovery)
                .add("isHealthy", isHealthy)
                .toString();
    }
}
