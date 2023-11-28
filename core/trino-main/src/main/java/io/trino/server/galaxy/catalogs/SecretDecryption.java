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
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.catalog.EncryptedSecret;
import io.starburst.stargate.catalog.QueryCatalog;
import io.starburst.stargate.crypto.SecretEncryptionContext;
import io.starburst.stargate.crypto.SecretSealer;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.TrinoPlaneId;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Maps.transformValues;

public final class SecretDecryption
{
    private SecretDecryption() {}

    public static QueryCatalog decryptCatalog(SecretSealer secretSealer, AccountId accountId, TrinoPlaneId trinoPlaneId, QueryCatalog queryCatalog)
    {
        Map<String, String> decryptedProperties = decryptSecrets(secretSealer, accountId, trinoPlaneId, queryCatalog);
        return new QueryCatalog(queryCatalog.catalogId(), queryCatalog.version(), queryCatalog.catalogName(), queryCatalog.connectorName(), queryCatalog.readOnly(), decryptedProperties, queryCatalog.secretsMap(), queryCatalog.secrets());
    }

    private static Map<String, String> decryptSecrets(SecretSealer secretSealer, AccountId accountId, TrinoPlaneId trinoPlaneId, QueryCatalog queryCatalog)
    {
        return ImmutableMap.copyOf(transformValues(queryCatalog.properties(), propertyValue -> {
            String decryptedPropertyValue = propertyValue;
            for (EncryptedSecret secret : queryCatalog.secrets().orElseGet(ImmutableSet::of)) {
                if (decryptedPropertyValue.contains(secret.placeholderText())) {
                    Map<String, String> metadataEncryptionContext = SecretEncryptionContext.forVerifier(accountId, trinoPlaneId, Optional.of(queryCatalog.catalogName()), secret.secretName());
                    String decryptedValue = secretSealer.unsealSecret(SecretSealer.SealedSecret.fromString(secret.encryptedValue()), metadataEncryptionContext);
                    decryptedPropertyValue = decryptedPropertyValue.replace(secret.placeholderText(), decryptedValue);
                }
            }
            return decryptedPropertyValue;
        }));
    }
}
