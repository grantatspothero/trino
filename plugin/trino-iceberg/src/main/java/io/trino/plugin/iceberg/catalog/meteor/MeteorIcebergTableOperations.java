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

import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;

import static io.trino.plugin.base.galaxy.GalaxyCatalogVersionUtils.getRequiredCatalogId;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MeteorIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private final MeteorCatalogClient catalogClient;
    private final CatalogHandle catalogHandle;
    private String metadataLocation;

    protected MeteorIcebergTableOperations(
            MeteorCatalogClient catalogClient,
            FileIO fileIo,
            ConnectorSession session,
            String database,
            CatalogHandle catalogHandle,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        this.catalogClient = requireNonNull(catalogClient, "catalogClient is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
    }

    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        if (metadataLocation == null || invalidateCaches) {
            metadataLocation = catalogClient.fetchMetadataLocation(session.getIdentity(), getRequiredCatalogId(catalogHandle.getVersion()).toString(), new SchemaTableName(database, tableName));
            if (metadataLocation == null) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, format("Failed to fetch table %s metadata location. Either it is not present or state server is not responding", getSchemaTableName()));
            }
        }

        return metadataLocation;
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "commit is not supported for Iceberg Meteor catalog");
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "commit is not supported for Iceberg Meteor catalog");
    }

    @Override
    protected void commitMaterializedViewRefresh(TableMetadata base, TableMetadata metadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "commit is not supported for Iceberg Meteor catalog");
    }
}
