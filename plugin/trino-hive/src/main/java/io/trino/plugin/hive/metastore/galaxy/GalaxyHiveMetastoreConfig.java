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
package io.trino.plugin.hive.metastore.galaxy;

import io.airlift.configuration.Config;
import io.starburst.stargate.metastore.client.MetastoreId;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import java.net.URI;

public class GalaxyHiveMetastoreConfig
{
    private MetastoreId metastoreId;
    private String sharedSecret;
    private URI serverUri;
    private String defaultDataDirectory;

    @NotNull
    public MetastoreId getMetastoreId()
    {
        return metastoreId;
    }

    @Config("galaxy.metastore.metastore-id")
    public GalaxyHiveMetastoreConfig setMetastoreId(MetastoreId metastoreId)
    {
        this.metastoreId = metastoreId;
        return this;
    }

    @NotNull
    @Size(min = 64, max = 64)
    @Pattern(regexp = "[0-9a-fA-F]+")
    public String getSharedSecret()
    {
        return sharedSecret;
    }

    @Config("galaxy.metastore.shared-secret")
    public GalaxyHiveMetastoreConfig setSharedSecret(String sharedSecret)
    {
        this.sharedSecret = sharedSecret;
        return this;
    }

    @NotNull
    public URI getServerUri()
    {
        return serverUri;
    }

    @Config("galaxy.metastore.server-uri")
    public GalaxyHiveMetastoreConfig setServerUri(URI serverUri)
    {
        this.serverUri = serverUri;
        return this;
    }

    @NotNull
    public String getDefaultDataDirectory()
    {
        return defaultDataDirectory;
    }

    @Config("galaxy.metastore.default-data-dir")
    public GalaxyHiveMetastoreConfig setDefaultDataDirectory(String defaultDataDirectory)
    {
        this.defaultDataDirectory = defaultDataDirectory;
        return this;
    }
}
