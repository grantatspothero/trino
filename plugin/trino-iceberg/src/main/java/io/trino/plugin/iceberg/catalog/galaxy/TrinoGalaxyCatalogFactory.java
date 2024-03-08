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
package io.trino.plugin.iceberg.catalog.galaxy;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static java.util.Objects.requireNonNull;

public class TrinoGalaxyCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogName catalogName;
    private final TypeManager typeManager;
    private final HiveMetastoreFactory metastoreFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final boolean isUniqueTableLocation;
    private final boolean cacheTableMetadata;
    private final boolean hideMaterializedViewStorageTable;

    @Inject
    public TrinoGalaxyCatalogFactory(
            CatalogName catalogName,
            TypeManager typeManager,
            HiveMetastoreFactory metastoreFactory,
            TrinoFileSystemFactory fileSystemFactory,
            IcebergTableOperationsProvider tableOperationsProvider,
            IcebergConfig config,
            IcebergGalaxyCatalogConfig galaxyCatalogConfig)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationProvider is null");
        this.isUniqueTableLocation = config.isUniqueTableLocation();
        this.cacheTableMetadata = galaxyCatalogConfig.isCacheTableMetadata();
        this.hideMaterializedViewStorageTable = config.isHideMaterializedViewStorageTable();
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoGalaxyCatalog(
                catalogName,
                typeManager,
                createPerTransactionCache(metastoreFactory.createMetastore(Optional.empty()), 1000),
                fileSystemFactory,
                tableOperationsProvider,
                isUniqueTableLocation,
                cacheTableMetadata,
                hideMaterializedViewStorageTable);
    }
}
