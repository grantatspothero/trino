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
import io.airlift.stats.TimeStat;
import io.starburst.stargate.catalog.CatalogVersionConfigurationApi;
import io.starburst.stargate.catalog.DeploymentType;
import io.starburst.stargate.catalog.QueryCatalog;
import io.starburst.stargate.crypto.SecretSealer;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogVersion;
import io.starburst.stargate.id.CloudRegionId;
import io.starburst.stargate.id.TrinoPlaneId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.server.galaxy.GalaxyConfig;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.ConnectorName;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.server.galaxy.catalogs.CatalogVersioningUtils.toCatalogHandle;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EncryptedSecretsGalaxyCatalogInfoSupplier
        implements GalaxyCatalogInfoSupplier
{
    private final TimeStat configureCatalog = new TimeStat(MILLISECONDS);
    private final TimeStat decryptCatalogSecrets = new TimeStat(MILLISECONDS);
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
        QueryCatalog queryCatalog = decryptCatalogSecrets(
                galaxyCatalogArgs.accountId(),
                configureCatalog(
                        dispatchSession,
                        galaxyCatalogArgs.accountId(),
                        galaxyCatalogArgs.catalogVersion()));

        Map<String, String> properties = new HashMap<>(queryCatalog.properties());
        properties.remove("galaxy.catalog-id");
        return new GalaxyCatalogInfo(
                new CatalogProperties(
                        toCatalogHandle(queryCatalog.catalogName(), galaxyCatalogArgs.catalogVersion()),
                        new ConnectorName(queryCatalog.connectorName()),
                        ImmutableMap.copyOf(properties)),
                queryCatalog.readOnly(),
                queryCatalog.sharedSchema());
    }

    private QueryCatalog configureCatalog(DispatchSession dispatchSession, AccountId accountId, CatalogVersion catalogVersion)
    {
        try (var ignored = configureCatalog.time()) {
            return catalogVersionConfigurationApi.configureCatalog(
                    dispatchSession,
                    accountId,
                    catalogVersion,
                    cloudRegionId,
                    Optional.of(trinoPlaneId),
                    deploymentType);
        }
    }

    private QueryCatalog decryptCatalogSecrets(AccountId accountId, QueryCatalog queryCatalog)
    {
        try (var ignored = decryptCatalogSecrets.time()) {
            return SecretDecryption.decryptCatalog(
                    secretSealer,
                    accountId,
                    queryCatalog,
                    decryptionContextProvider);
        }
    }

    @Managed
    @Nested
    public TimeStat getConfigureCatalog()
    {
        return configureCatalog;
    }

    @Managed
    @Nested
    public TimeStat getDecryptCatalogSecrets()
    {
        return decryptCatalogSecrets;
    }
}
