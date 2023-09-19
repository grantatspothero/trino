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
package io.trino.plugin.iceberg.catalog.meteor;

import com.google.inject.Inject;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public class TrinoMeteorCatalogFactory
        implements TrinoCatalogFactory
{
    private final CatalogHandle catalogHandle;
    private final IcebergTableOperationsProvider tableOperationsProvider;

    private final MeteorCatalogClient restCatalogClient;

    @Inject
    public TrinoMeteorCatalogFactory(
            MeteorCatalogClient restCatalogClient,
            CatalogHandle catalogHandle,
            IcebergTableOperationsProvider tableOperationsProvider)
    {
        this.restCatalogClient = requireNonNull(restCatalogClient, "restCatalogClient is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
    }

    @Override
    public synchronized TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoMeteorCatalog(restCatalogClient, catalogHandle, tableOperationsProvider);
    }
}
