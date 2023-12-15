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
package io.trino.server.galaxy.catalogs;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.starburst.stargate.catalog.DeploymentType;
import io.starburst.stargate.id.TrinoPlaneId;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class LiveCatalogsConfig
{
    private DeploymentType deploymentType;
    private URI catalogConfigurationURI;
    private Duration maxStaleness = new Duration(24, TimeUnit.HOURS);
    private Duration oldVersionStaleness = new Duration(5, TimeUnit.MINUTES);
    private boolean queryRunnerTesting;
    private TrinoPlaneId trinoPlaneId;
    private boolean useKmsCrypto;

    @NotNull
    public DeploymentType getDeploymentType()
    {
        return deploymentType;
    }

    @Config("galaxy.deployment-type")
    public LiveCatalogsConfig setDeploymentType(DeploymentType deploymentType)
    {
        this.deploymentType = deploymentType;
        return this;
    }

    @NotNull
    public TrinoPlaneId getTrinoPlaneId()
    {
        return trinoPlaneId;
    }

    @Config("trino.plane-id")
    public LiveCatalogsConfig setTrinoPlaneId(TrinoPlaneId trinoPlaneId)
    {
        this.trinoPlaneId = trinoPlaneId;
        return this;
    }

    public boolean getUseKmsCrypto()
    {
        return useKmsCrypto;
    }

    @Config("kms-crypto.enabled")
    public LiveCatalogsConfig setUseKmsCrypto(boolean useKmsCrypto)
    {
        this.useKmsCrypto = useKmsCrypto;
        return this;
    }

    @NotNull
    public URI getCatalogConfigurationURI()
    {
        return catalogConfigurationURI;
    }

    @Config("galaxy.catalog-configuration-uri")
    public LiveCatalogsConfig setCatalogConfigurationURI(URI catalogConfigurationURI)
    {
        this.catalogConfigurationURI = catalogConfigurationURI;
        return this;
    }

    @Config("galaxy.live-catalog.max.staleness")
    public LiveCatalogsConfig setMaxCatalogStaleness(Duration maxStaleness)
    {
        this.maxStaleness = maxStaleness;
        return this;
    }

    @NotNull
    @MinDuration("1m")
    public Duration getMaxCatalogStaleness()
    {
        return maxStaleness;
    }

    @Config("galaxy.live-catalog.old.version.staleness")
    public LiveCatalogsConfig setOldVersionStaleness(Duration oldVersionStaleness)
    {
        this.oldVersionStaleness = oldVersionStaleness;
        return this;
    }

    @NotNull
    @MinDuration("10s")
    public Duration getOldVersionStaleness()
    {
        return oldVersionStaleness;
    }

    @Config("galaxy.testing.query.runner")
    public void setQueryRunnerTesting(boolean queryRunnerTesting)
    {
        this.queryRunnerTesting = queryRunnerTesting;
    }

    public boolean isQueryRunnerTesting()
    {
        return queryRunnerTesting;
    }
}
