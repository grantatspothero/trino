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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.metastore.client.BadMetastoreRequestException;
import io.starburst.stargate.metastore.client.MetastoreConflictException;
import io.trino.annotation.NotThreadSafe;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.plugin.iceberg.catalog.hms.AbstractMetastoreTableOperations;
import io.trino.plugin.iceberg.util.HiveSchemaUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static io.trino.plugin.iceberg.IcebergUtil.COLUMN_TRINO_NOT_NULL_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.COLUMN_TRINO_TYPE_ID_PROPERTY;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;

@NotThreadSafe
public class GalaxyMetastoreTableOperations
        extends AbstractMetastoreTableOperations
{
    private final TypeManager typeManager;
    private final boolean cacheTableMetadata;

    public GalaxyMetastoreTableOperations(
            TypeManager typeManager,
            boolean cacheTableMetadata,
            FileIO fileIo,
            CachingHiveMetastore metastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, metastore, session, database, table, owner, location);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheTableMetadata = cacheTableMetadata;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        Table currentTable = getTable();
        commitTableUpdate(currentTable, metadata, (table, newMetadataLocation) -> Table.builder(table)
                .apply(builder -> updateMetastoreTable(builder, metadata, newMetadataLocation, Optional.of(currentMetadataLocation)))
                .build());
    }

    @Override
    protected void commitMaterializedViewRefresh(TableMetadata base, TableMetadata metadata)
    {
        Table materializedView = getTable(database, tableNameFrom(tableName));
        commitTableUpdate(materializedView, metadata, (table, newMetadataLocation) -> Table.builder(table)
                .apply(builder -> builder
                        .setParameter(METADATA_LOCATION_PROP, newMetadataLocation)
                        .setParameter(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation))
                .build());
    }

    private void commitTableUpdate(Table table, TableMetadata metadata, BiFunction<Table, String, Table> tableUpdateFunction)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version.orElseThrow() + 1);

        try {
            Table updatedTable = tableUpdateFunction.apply(table, newMetadataLocation);
            metastore.replaceTable(table.getDatabaseName(), table.getTableName(), updatedTable, null);
        }
        catch (TrinoException e) {
            if (e.getCause() instanceof BadMetastoreRequestException) {
                throw e;
            }
            if (e.getCause() instanceof MetastoreConflictException) {
                throw new CommitFailedException(e, "%s", e.getCause().getMessage());
            }
            throw new CommitStateUnknownException(e);
        }
    }

    @Override
    protected Table.Builder updateMetastoreTable(Table.Builder builder, TableMetadata metadata, String metadataLocation, Optional<String> previousMetadataLocation)
    {
        builder = super.updateMetastoreTable(builder, metadata, metadataLocation, previousMetadataLocation);
        if (!cacheTableMetadata) {
            return builder;
        }
        return builder
                .setParameter(TABLE_COMMENT, Optional.ofNullable(metadata.properties().get(TABLE_COMMENT)))
                .setDataColumns(metastoreColumns(metadata));
    }

    private List<Column> metastoreColumns(TableMetadata metadata)
    {
        return metadata.schema().columns().stream()
                .map(icebergColumn -> toMetastoreColumn(icebergColumn, typeManager))
                .collect(toImmutableList());
    }

    /**
     * Converts Iceberg column information to metastore column storing sufficient information to reconstruct {@link ColumnMetadata}
     * (and thus answer {@code information_schema.columns} queries).
     */
    @VisibleForTesting
    static Column toMetastoreColumn(Types.NestedField icebergColumn, TypeManager typeManager)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builderWithExpectedSize(2);
        String trinoTypeId = TypeConverter.toTrinoType(icebergColumn.type(), typeManager).getTypeId().getId();
        properties.put(COLUMN_TRINO_TYPE_ID_PROPERTY, trinoTypeId);
        if (icebergColumn.isRequired()) {
            properties.put(COLUMN_TRINO_NOT_NULL_PROPERTY, "true");
        }
        return new Column(
                icebergColumn.name(),
                toHiveType(HiveSchemaUtil.convert(icebergColumn.type())),
                Optional.ofNullable(icebergColumn.doc()),
                properties.buildOrThrow());
    }
}
