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

import javax.validation.constraints.NotEmpty;

public class GalaxyConfig
{
    private String accountId;
    private String clusterId;
    private String deploymentId;

    @NotEmpty
    public String getAccountId()
    {
        return accountId;
    }

    @Config("galaxy.account-id")
    public GalaxyConfig setAccountId(String accountId)
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
    public GalaxyConfig setClusterId(String clusterId)
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
    public GalaxyConfig setDeploymentId(String deploymentId)
    {
        this.deploymentId = deploymentId;
        return this;
    }
}
