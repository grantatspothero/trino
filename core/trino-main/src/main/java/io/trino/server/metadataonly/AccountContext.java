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
package io.trino.server.metadataonly;

import io.starburst.stargate.catalog.QueryCatalog;
import io.starburst.stargate.id.AccountId;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

final class AccountContext
{
    private final AccountId accountId;
    private final Map<String, QueryCatalog> catalogs;
    private final UnaryOperator<QueryCatalog> decryptProc;

    AccountContext(AccountId accountId, List<QueryCatalog> catalogs, UnaryOperator<QueryCatalog> decryptProc)
    {
        this.accountId = requireNonNull(accountId, "accountId is null");
        this.decryptProc = requireNonNull(decryptProc, "decryptProc is null");
        this.catalogs = catalogs.stream().collect(toImmutableMap(QueryCatalog::catalogName, Function.identity()));
    }

    Map<String, String> decryptCatalogProperties(String catalogName)
    {
        QueryCatalog queryCatalog = catalogs.get(catalogName);
        requireNonNull(queryCatalog, () -> "Catalog not found. CatalogName: %s AccountID: %s".formatted(catalogName, accountId));
        return decryptProc.apply(queryCatalog).properties();
    }

    AccountId accountId()
    {
        return accountId;
    }
}
