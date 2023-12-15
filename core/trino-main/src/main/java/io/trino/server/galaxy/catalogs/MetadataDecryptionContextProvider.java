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
import io.starburst.stargate.id.TrinoPlaneId;
import io.trino.server.metadataonly.MetadataOnlyConfig;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MetadataDecryptionContextProvider
        implements DecryptionContextProvider
{
    private final TrinoPlaneId trinoPlaneId;

    @Inject
    public MetadataDecryptionContextProvider(MetadataOnlyConfig metadataOnlyConfig)
    {
        requireNonNull(metadataOnlyConfig, "liveCatalogConfig is null");
        trinoPlaneId = requireNonNull(metadataOnlyConfig.getTrinoPlaneId(), "trinoPlaneId is null");
    }

    @Override
    public Map<String, String> decryptionContextFor(AccountId accountId, QueryCatalog queryCatalog, EncryptedSecret secret)
    {
        //TODO before Live Catalog use in Metadata service, consider transition to service plane and new decryption at the same time
        return SecretEncryptionContext.forVerifier(accountId, trinoPlaneId, Optional.of(queryCatalog.catalogName()), secret.secretName());
    }
}
