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
package io.trino.server.galaxy.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HeartbeatEvent
{
    private final long durationMillis;
    private final String clusterId;
    private final String accountId;
    private final String deploymentId;
    private final Optional<String> cloudRegionId;  // optional until both stargate and stargate-trino are sync'd in prod
    private final String variant;
    private final String role;
    private final String podName;
    private final String podIp;
    private final List<CatalogMetricsSnapshot> catalogMetrics;

    public HeartbeatEvent(long durationMillis, String clusterId, String accountId, String deploymentId, Optional<String> cloudRegionId, String variant, String role, String podName, String podIp, List<CatalogMetricsSnapshot> catalogMetrics)
    {
        this.durationMillis = durationMillis;
        this.clusterId = requireNonNull(clusterId, "clusterId is null");
        this.accountId = requireNonNull(accountId, "accountId is null");
        this.deploymentId = requireNonNull(deploymentId, "deploymentId is null");
        this.cloudRegionId = requireNonNull(cloudRegionId, "cloudRegionId is null");
        this.variant = requireNonNull(variant, "variant is null");
        this.role = requireNonNull(role, "role is null");
        this.podName = requireNonNull(podName, "podName is null");
        this.podIp = requireNonNull(podIp, "podIp is null");
        this.catalogMetrics = ImmutableList.copyOf(requireNonNull(catalogMetrics, "catalogMetricsMap is null"));
    }

    // TODO: Metronome is still using the legacy name
    @JsonProperty
    public String getWarehouseId()
    {
        return clusterId;
    }

    @JsonProperty
    public String getClusterId()
    {
        return clusterId;
    }

    @JsonProperty
    public String getAccountId()
    {
        return accountId;
    }

    @JsonProperty
    public String getDeploymentId()
    {
        return deploymentId;
    }

    @JsonProperty
    public Optional<String> getCloudRegionId()
    {
        return cloudRegionId;
    }

    @JsonProperty
    public String getVariant()
    {
        return variant;
    }

    @JsonProperty
    public String getRole()
    {
        return role;
    }

    @JsonProperty
    public String getPodName()
    {
        return podName;
    }

    @JsonProperty
    public String getPodIp()
    {
        return podIp;
    }

    @JsonProperty
    public long getDurationMillis()
    {
        return durationMillis;
    }

    @JsonProperty
    public List<CatalogMetricsSnapshot> getCatalogMetrics()
    {
        return catalogMetrics;
    }
}
