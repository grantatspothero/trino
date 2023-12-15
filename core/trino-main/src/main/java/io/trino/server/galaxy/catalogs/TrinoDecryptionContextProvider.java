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

import com.google.inject.Inject;
import io.starburst.stargate.catalog.EncryptedSecret;
import io.starburst.stargate.catalog.QueryCatalog;
import io.starburst.stargate.crypto.SecretEncryptionContext;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.DeploymentId;
import io.starburst.stargate.id.TrinoPlaneId;
import io.trino.server.galaxy.GalaxyConfig;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TrinoDecryptionContextProvider
        implements DecryptionContextProvider
{
    private final TrinoPlaneId trinoPlaneId;
    private final DeploymentId deploymentId;

    @Inject
    public TrinoDecryptionContextProvider(GalaxyConfig galaxyConfig, LiveCatalogsConfig liveCatalogsConfig)
    {
        requireNonNull(galaxyConfig, "galaxyConfig is null");
        requireNonNull(liveCatalogsConfig, "liveCatalogConfig is null");
        trinoPlaneId = requireNonNull(liveCatalogsConfig.getTrinoPlaneId(), "trinoPlaneId is null");
        deploymentId = new DeploymentId(requireNonNull(galaxyConfig.getDeploymentId()));
    }

    @Override
    public Map<String, String> decryptionContextFor(AccountId accountId, QueryCatalog queryCatalog, EncryptedSecret secret)
    {
        return SecretEncryptionContext.forTrino(accountId, trinoPlaneId, deploymentId, queryCatalog.catalogName(), secret.secretName());
    }
}
