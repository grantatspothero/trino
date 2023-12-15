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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.starburst.stargate.catalog.CatalogVersionConfigurationApi;
import io.starburst.stargate.catalog.DeploymentType;
import io.starburst.stargate.catalog.QueryCatalog;
import io.starburst.stargate.crypto.SecretSealer;
import io.starburst.stargate.id.CloudRegionId;
import io.starburst.stargate.id.TrinoPlaneId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.connector.CatalogProperties;
import io.trino.connector.ConnectorName;
import io.trino.server.galaxy.GalaxyConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.server.galaxy.catalogs.CatalogVersioningUtils.toCatalogHandle;
import static java.util.Objects.requireNonNull;

public class EncryptedSecretsGalaxyCatalogInfoSupplier
        implements GalaxyCatalogInfoSupplier
{
    // used to configure the catalog
    private final CatalogVersionConfigurationApi catalogVersionConfigurationApi;
    private final CloudRegionId cloudRegionId;
    private final DeploymentType deploymentType;
    // used for secret unsealing
    private final SecretSealer secretSealer;
    private final TrinoPlaneId trinoPlaneId;
    private final DecryptionContextProvider decryptionContextProvider;

    @Inject
    public EncryptedSecretsGalaxyCatalogInfoSupplier(
            CatalogVersionConfigurationApi catalogVersionConfigurationApi,
            SecretSealer secretSealer,
            GalaxyConfig galaxyConfig,
            LiveCatalogsConfig liveCatalogsConfig,
            DecryptionContextProvider decryptionContextProvider)
    {
        this.catalogVersionConfigurationApi = requireNonNull(catalogVersionConfigurationApi, "catalogVersionConfigurationApi is null");
        this.secretSealer = requireNonNull(secretSealer, "secretSealer is null");
        this.cloudRegionId = new CloudRegionId(galaxyConfig.getCloudRegionId());
        this.trinoPlaneId = liveCatalogsConfig.getTrinoPlaneId();
        this.deploymentType = liveCatalogsConfig.getDeploymentType();
        this.decryptionContextProvider = requireNonNull(decryptionContextProvider, "decryptionContextProvider is null");
    }

    @Override
    public GalaxyCatalogInfo getGalaxyCatalogInfo(DispatchSession dispatchSession, GalaxyCatalogArgs galaxyCatalogArgs)
    {
        QueryCatalog queryCatalog = SecretDecryption.decryptCatalog(
                secretSealer,
                galaxyCatalogArgs.accountId(),
                catalogVersionConfigurationApi.configureCatalog(
                        dispatchSession,
                        galaxyCatalogArgs.accountId(),
                        galaxyCatalogArgs.catalogVersion(),
                        cloudRegionId,
                        Optional.of(trinoPlaneId),
                        deploymentType),
                decryptionContextProvider);
        Map<String, String> properties = new HashMap<>(queryCatalog.properties());
        properties.remove("galaxy.catalog-id");
        return new GalaxyCatalogInfo(
                new CatalogProperties(
                        toCatalogHandle(queryCatalog.catalogName(), galaxyCatalogArgs.catalogVersion()),
                        new ConnectorName(queryCatalog.connectorName()),
                        ImmutableMap.copyOf(properties)),
                queryCatalog.readOnly());
    }
}
