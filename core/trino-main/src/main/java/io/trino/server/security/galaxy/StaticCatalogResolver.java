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
package io.trino.server.security.galaxy;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import io.trino.server.galaxy.catalogs.CatalogResolver;
import io.trino.transaction.TransactionId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class StaticCatalogResolver
        implements CatalogResolver
{
    private final BiMap<String, CatalogId> catalogNamesToIds;
    private final Set<String> readOnlyCatalogs;
    private final Map<String, SharedSchemaNameAndAccepted> sharedSchemas;

    @Inject
    public StaticCatalogResolver(GalaxyAccessControlConfig config, GalaxySystemAccessControlConfig systemConfig)
    {
        this(config.getCatalogNames(), systemConfig.getReadOnlyCatalogs(), systemConfig.getSharedCatalogSchemaNames());
    }

    public StaticCatalogResolver(Map<String, CatalogId> catalogNamesToIds, Set<String> readOnlyCatalogs, Map<String, SharedSchemaNameAndAccepted> sharedSchemas)
    {
        this.catalogNamesToIds = ImmutableBiMap.copyOf(catalogNamesToIds);
        this.readOnlyCatalogs = ImmutableSet.copyOf(readOnlyCatalogs);
        this.sharedSchemas = ImmutableMap.copyOf(sharedSchemas);
    }

    public Set<String> getCatalogNames()
    {
        return catalogNamesToIds.keySet();
    }

    @Override
    public boolean isReadOnlyCatalog(Optional<TransactionId> transactionId, String catalogName)
    {
        return readOnlyCatalogs.contains(catalogName);
    }

    @Override
    public Optional<CatalogId> getCatalogId(Optional<TransactionId> transactionId, String catalogName)
    {
        return Optional.ofNullable(catalogNamesToIds.get(catalogName));
    }

    @Override
    public Optional<String> getCatalogName(Optional<TransactionId> transactionId, CatalogId catalogId)
    {
        return Optional.ofNullable(catalogNamesToIds.inverse().get(catalogId));
    }

    @Override
    public Optional<SharedSchemaNameAndAccepted> getSharedSchemaForCatalog(Optional<TransactionId> transactionId, String catalogName)
    {
        return Optional.ofNullable(sharedSchemas.get(catalogName));
    }
}
