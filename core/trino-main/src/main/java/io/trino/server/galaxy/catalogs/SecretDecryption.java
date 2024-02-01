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
import io.starburst.stargate.crypto.SecretSealer;
import io.starburst.stargate.id.AccountId;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Maps.transformValues;

public final class SecretDecryption
{
    private SecretDecryption() {}

    public static QueryCatalog decryptCatalog(SecretSealer secretSealer, AccountId accountId, QueryCatalog queryCatalog, DecryptionContextProvider decryptionContextProvider)
    {
        Map<String, String> decryptedProperties = decryptSecrets(secretSealer, accountId, queryCatalog, decryptionContextProvider);
        return new QueryCatalog(queryCatalog.catalogId(), queryCatalog.version(), queryCatalog.catalogName(), queryCatalog.connectorName(), queryCatalog.readOnly(), decryptedProperties, queryCatalog.secretsMap(), queryCatalog.secrets(), Optional.empty());
    }

    private static Map<String, String> decryptSecrets(SecretSealer secretSealer, AccountId accountId, QueryCatalog queryCatalog, DecryptionContextProvider decryptionContextProvider)
    {
        return ImmutableMap.copyOf(transformValues(queryCatalog.properties(), propertyValue -> {
            String decryptedPropertyValue = propertyValue;
            for (EncryptedSecret secret : queryCatalog.secrets().orElseGet(ImmutableSet::of)) {
                if (decryptedPropertyValue.contains(secret.placeholderText())) {
                    Map<String, String> decryptionContext = decryptionContextProvider.decryptionContextFor(accountId, queryCatalog, secret);
                    String decryptedValue = secretSealer.unsealSecret(SecretSealer.SealedSecret.fromString(secret.encryptedValue()), decryptionContext);
                    decryptedPropertyValue = decryptedPropertyValue.replace(secret.placeholderText(), decryptedValue);
                }
            }
            return decryptedPropertyValue;
        }));
    }
}
