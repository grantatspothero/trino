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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class IcebergCacheTableId
{
    private final CatalogHandle catalog;
    private final String schemaName;
    private final String tableName;
    private final String tableLocation;
    private final TupleDomain<CacheColumnId> unenforcedPredicate;
    private final Map<String, String> storageProperties;

    public IcebergCacheTableId(
            CatalogHandle catalog,
            String schemaName,
            String tableName,
            String tableLocation,
            TupleDomain<CacheColumnId> unenforcedPredicate,
            Map<String, String> storageProperties)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
        this.storageProperties = requireNonNull(storageProperties, "storageProperties is null");
    }

    @JsonProperty
    public CatalogHandle getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getTableLocation()
    {
        return tableLocation;
    }

    @JsonProperty
    public TupleDomain<CacheColumnId> getUnenforcedPredicate()
    {
        return unenforcedPredicate;
    }

    @JsonProperty
    public Map<String, String> getStorageProperties()
    {
        return storageProperties;
    }
}
