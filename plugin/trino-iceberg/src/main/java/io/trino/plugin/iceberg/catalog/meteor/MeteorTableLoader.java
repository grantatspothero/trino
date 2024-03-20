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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.plugin.iceberg.TrinoMetricsReporter.TRINO_METRICS_REPORTER;
import static java.util.Objects.requireNonNull;

public final class MeteorTableLoader
{
    private static final String DEAD_LETTER_TABLE_SUFFIX = "__errors";

    private final TrinoFileSystemFactory fileSystemFactory;
    private final MeteorCatalogClient apiClient;
    private final CatalogHandle catalogHandle;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public MeteorTableLoader(
            TrinoFileSystemFactory fileSystemFactory,
            MeteorCatalogClient apiClient,
            CatalogHandle catalogHandle)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.apiClient = requireNonNull(apiClient, "apiClient is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
    }

    public Optional<Table> loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        if (!schemaTableName.getTableName().endsWith(DEAD_LETTER_TABLE_SUFFIX)) {
            return Optional.empty();
        }

        TableMetadata metadata;
        try {
            metadata = tableMetadataCache.computeIfAbsent(
                    schemaTableName,
                    ignore -> createOperations(session, schemaTableName).current());
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }

        IcebergTableOperations operations = createOperations(session, schemaTableName);
        operations.initializeFromMetadata(metadata);
        return Optional.of(new BaseTable(operations, quotedTableName(schemaTableName), TRINO_METRICS_REPORTER));
    }

    private IcebergTableOperations createOperations(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return new MeteorIcebergTableOperations(
                apiClient,
                new ForwardingFileIo(fileSystemFactory.create(session)),
                session,
                schemaTableName.getSchemaName(),
                catalogHandle,
                schemaTableName.getTableName(),
                Optional.empty(),
                Optional.empty());
    }
}
