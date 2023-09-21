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
package io.trino.plugin.objectstore;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

public class GalaxyAccountWorkConfig
{
    private String clusterId;
    private URI accountUri;

    @NotNull
    public URI getAccountUri()
    {
        return accountUri;
    }

    @Config("galaxy.account-url")
    @ConfigDescription("The url of the Galaxy domain this cluster belongs to")
    public GalaxyAccountWorkConfig setAccountUri(URI accountUri)
    {
        this.accountUri = accountUri;
        return this;
    }

    @NotEmpty
    public String getClusterId()
    {
        return clusterId;
    }

    @Config("galaxy.cluster-id")
    @ConfigDescription("The ClusterId this cluster is assigned in the Galaxy database")
    public GalaxyAccountWorkConfig setClusterId(String clusterId)
    {
        this.clusterId = clusterId;
        return this;
    }
}
