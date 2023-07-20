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

package io.trino.server.resultscache;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class ResultsCacheConfig
{
    private int cacheUploadThreads = 5;
    private String cacheEndpoint = "http://results-cache.trino-results-cache.svc:8080";
    private boolean galaxyEnabled;
    private Optional<String> clusterId = Optional.empty();
    private Optional<String> deploymentId = Optional.empty();

    @Positive
    public int getCacheUploadThreads()
    {
        return cacheUploadThreads;
    }

    @Config("galaxy.results-cache.upload.threads")
    @ConfigDescription("Size of thread pool for uploading ResultSets to the ResultSet Cache")
    public ResultsCacheConfig setCacheUploadThreads(int cacheUploadThreads)
    {
        this.cacheUploadThreads = cacheUploadThreads;
        return this;
    }

    @NotNull
    public String getCacheEndpoint()
    {
        return cacheEndpoint;
    }

    @Config("galaxy-results-cache.upload.endpoint")
    @ConfigDescription("Endpoint of local ResultSet Cache Instance")
    public ResultsCacheConfig setCacheEndpoint(String cacheEndpoint)
    {
        this.cacheEndpoint = cacheEndpoint;
        return this;
    }

    public boolean isGalaxyEnabled()
    {
        return galaxyEnabled;
    }

    @Config("galaxy.enabled")
    public ResultsCacheConfig setGalaxyEnabled(boolean galaxyEnabled)
    {
        this.galaxyEnabled = galaxyEnabled;
        return this;
    }

    @NotNull
    public Optional<String> getClusterId()
    {
        return clusterId;
    }

    @Config("galaxy.cluster-id")
    public ResultsCacheConfig setClusterId(String clusterId)
    {
        this.clusterId = Optional.of(clusterId);
        return this;
    }

    @NotNull
    public Optional<String> getDeploymentId()
    {
        return deploymentId;
    }

    @Config("galaxy.deployment-id")
    public ResultsCacheConfig setDeploymentId(String deploymentId)
    {
        this.deploymentId = Optional.of(deploymentId);
        return this;
    }

    @PostConstruct
    public void validate()
    {
        if (galaxyEnabled) {
            checkState(clusterId.isPresent() && deploymentId.isPresent(),
                    "Cluster and Deployment Id's must be set when galaxy is enabled.");
        }
    }
}
