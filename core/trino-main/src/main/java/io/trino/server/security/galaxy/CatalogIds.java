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
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.id.CatalogId;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CatalogIds
{
    private final BiMap<String, CatalogId> catalogNamesToIds;
    private final Set<String> readOnlyCatalogs;

    @Inject
    public CatalogIds(GalaxyAccessControlConfig config)
    {
        this(config.getCatalogNames(), config.getReadOnlyCatalogs());
    }

    public CatalogIds(Map<String, CatalogId> catalogNamesToIds, Set<String> readOnlyCatalogs)
    {
        this.catalogNamesToIds = ImmutableBiMap.copyOf(catalogNamesToIds);
        this.readOnlyCatalogs = ImmutableSet.copyOf(readOnlyCatalogs);
    }

    public Set<String> getCatalogNames()
    {
        return catalogNamesToIds.keySet();
    }

    public boolean isReadOnlyCatalog(String catalogName)
    {
        return readOnlyCatalogs.contains(catalogName);
    }

    public Optional<CatalogId> getCatalogId(String catalogName)
    {
        return Optional.ofNullable(catalogNamesToIds.get(catalogName));
    }

    public Optional<String> getCatalogName(CatalogId catalogId)
    {
        return Optional.ofNullable(catalogNamesToIds.inverse().get(catalogId));
    }
}
