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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.ViewReaderUtil.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergMaterializedViewAdditionalProperties.STORAGE_SCHEMA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.fromConnectorMaterializedViewDefinition;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.plugin.iceberg.IcebergUtil.validateTableCanBeDropped;
import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.ICEBERG_METASTORE_STORAGE_FORMAT;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.CatalogUtil.dropTableData;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.Transactions.createTableTransaction;

public class TrinoGalaxyCatalog
        extends AbstractTrinoCatalog
{
    private static final Logger log = Logger.get(TrinoGalaxyCatalog.class);
    private final CachingHiveMetastore metastore;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean useUniqueTableLocation;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public TrinoGalaxyCatalog(
            CatalogName catalogName,
            TypeManager typeManager,
            CachingHiveMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory,
            IcebergTableOperationsProvider tableOperationsProvider,
            boolean useUniqueTableLocation)
    {
        super(catalogName, typeManager, tableOperationsProvider, useUniqueTableLocation);
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
    }

    public CachingHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        // Needed only because of schemaExists check in beginCreateTable
        if (!namespace.equals(namespace.toLowerCase(ENGLISH))) {
            // Currently, Trino schemas are always lowercase, so this one cannot exist (https://github.com/trinodb/trino/issues/17)
            return false;
        }
        if (HiveUtil.isHiveSystemSchema(namespace)) {
            return false;
        }
        return metastore.getDatabase(namespace).isPresent();
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, String location, Map<String, String> properties)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, location, properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                Optional.of(session.getUser()),
                Optional.of(location));
        return createTableTransaction(schemaTableName.toString(), ops, metadata);
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName schemaTableName, String tableLocation, String metadataLocation)
    {
        io.trino.plugin.hive.metastore.Table.Builder table = io.trino.plugin.hive.metastore.Table.builder()
                .setDatabaseName(schemaTableName.getSchemaName())
                .setTableName(schemaTableName.getTableName())
                .setOwner(Optional.of("galaxy")) // not used by Galaxy
                // Table needs to be EXTERNAL, otherwise table rename in HMS would rename table directory and break table contents.
                .setTableType(TableType.EXTERNAL_TABLE.name())
                .withStorage(storage -> storage.setLocation(tableLocation))
                .withStorage(storage -> storage.setStorageFormat(ICEBERG_METASTORE_STORAGE_FORMAT))
                // This is a must-have property for the EXTERNAL_TABLE table type
                .setParameter("EXTERNAL", "TRUE")
                .setParameter(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                .setParameter(METADATA_LOCATION_PROP, metadataLocation);

        metastore.createTable(table.build(), NO_PRIVILEGES);
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        io.trino.plugin.hive.metastore.Table table = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        if (!isIcebergTable(table)) {
            throw new UnknownTableTypeException(schemaTableName);
        }

        metastore.dropTable(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                false /* do not delete data */);
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BaseTable table = (BaseTable) loadTable(session, schemaTableName);
        TableMetadata metadata = table.operations().current();
        validateTableCanBeDropped(table);

        io.trino.plugin.hive.metastore.Table metastoreTable = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        metastore.dropTable(schemaTableName.getSchemaName(), schemaTableName.getTableName(), false);

        // Use the Iceberg routine for dropping the table data because the data files
        // of the Iceberg table may be located in different locations
        dropTableData(table.io(), metadata);

        String location = metastoreTable.getStorage().getLocation();
        try {
            fileSystemFactory.create(session).deleteDirectory(location);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("Failed to delete directory %s of the table %s", location, schemaTableName), e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        return namespace.map(Stream::of)
                .orElseGet(() -> metastore.getAllDatabases().stream())
                .flatMap(schema -> metastore.getAllTables(schema).stream()
                        .map(table -> new SchemaTableName(schema, table)))
                .collect(toImmutableList());
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        metastore.renameTable(from.getSchemaName(), from.getTableName(), to.getSchemaName(), to.getTableName());
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableMetadata metadata = tableMetadataCache.computeIfAbsent(
                schemaTableName,
                ignore -> ((BaseTable) loadIcebergTable(this, tableOperationsProvider, session, schemaTableName)).operations().current());

        return getIcebergTableWithMetadata(this, tableOperationsProvider, session, schemaTableName, metadata);
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        metastore.commentTable(schemaTableName.getSchemaName(), schemaTableName.getTableName(), comment);
        Table icebergTable = loadTable(session, schemaTableName);
        comment.ifPresentOrElse(
                value -> icebergTable.updateProperties().set(TABLE_COMMENT, value).commit(),
                () -> icebergTable.updateProperties().remove(TABLE_COMMENT).commit());
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        icebergTable.updateSchema().updateColumnDoc(columnIdentity.getName(), comment.orElse(null)).commit();
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Database database = metastore.getDatabase(schemaTableName.getSchemaName())
                .orElseThrow(() -> new SchemaNotFoundException(schemaTableName.getSchemaName()));

        String databaseLocation = database.getLocation().orElseThrow(() ->
                new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location is not set", schemaTableName.getSchemaName())));

        String tableLocation = databaseLocation + "/" + schemaTableName.getTableName();

        if (useUniqueTableLocation) {
            tableLocation += "-" + randomUUID().toString().replace("-", "");
        }

        return tableLocation;
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewIdentifier)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        // Filter on ICEBERG_MATERIALIZED_VIEW_COMMENT is used to avoid listing hive views in case of a shared HMS and to distinguish from standard views
        return namespace.<List<String>>map(ImmutableList::of)
                .orElseGet(metastore::getAllDatabases)
                .stream()
                .flatMap(schema -> metastore.getTablesWithParameter(schema, TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT).stream()
                        .map(table -> new SchemaTableName(schema, table)))
                .collect(toImmutableList());
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        Optional<io.trino.plugin.hive.metastore.Table> existing = metastore.getTable(viewName.getSchemaName(), viewName.getTableName());

        if (existing.isPresent()) {
            if (!isTrinoMaterializedView(existing.get().getTableType(), existing.get().getParameters())) {
                throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Existing table is not a Materialized View: " + viewName);
            }
            if (!replace) {
                if (ignoreExisting) {
                    return;
                }
                throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + viewName);
            }
        }

        SchemaTableName storageTable = createMaterializedViewStorageTable(session, viewName, definition);

        // Create a view indicating the storage table
        Map<String, String> viewProperties = createMaterializedViewProperties(session, storageTable);
        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty());

        io.trino.plugin.hive.metastore.Table.Builder tableBuilder = io.trino.plugin.hive.metastore.Table.builder()
                .setDatabaseName(viewName.getSchemaName())
                .setTableName(viewName.getTableName())
                .setOwner(Optional.empty())
                .setTableType(VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(viewProperties)
                .withStorage(storage -> storage.setStorageFormat(VIEW_STORAGE_FORMAT))
                .withStorage(storage -> storage.setLocation(""))
                .setViewOriginalText(Optional.of(encodeMaterializedViewData(fromConnectorMaterializedViewDefinition(definition))))
                .setViewExpandedText(Optional.of("/* " + ICEBERG_MATERIALIZED_VIEW_COMMENT + " */"));
        io.trino.plugin.hive.metastore.Table table = tableBuilder.build();
        if (existing.isPresent()) {
            // drop the current storage table
            String oldStorageTable = existing.get().getParameters().get(STORAGE_TABLE);
            if (oldStorageTable != null) {
                String storageSchema = Optional.ofNullable(existing.get().getParameters().get(STORAGE_SCHEMA))
                        .orElse(viewName.getSchemaName());
                metastore.dropTable(storageSchema, oldStorageTable, true);
            }
            // Replace the existing view definition
            metastore.replaceTable(viewName.getSchemaName(), viewName.getTableName(), table, PrincipalPrivileges.NO_PRIVILEGES);
            return;
        }
        // create the view definition
        metastore.createTable(table, PrincipalPrivileges.NO_PRIVILEGES);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        io.trino.plugin.hive.metastore.Table view = metastore.getTable(viewName.getSchemaName(), viewName.getTableName())
                .orElseThrow(() -> new MaterializedViewNotFoundException(viewName));

        if (!isTrinoMaterializedView(view.getTableType(), view.getParameters())) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Not a Materialized View: " + viewName);
        }

        String storageTableName = view.getParameters().get(STORAGE_TABLE);
        if (storageTableName != null) {
            String storageSchema = Optional.ofNullable(view.getParameters().get(STORAGE_SCHEMA))
                    .orElse(viewName.getSchemaName());
            try {
                dropTable(session, new SchemaTableName(storageSchema, storageTableName));
            }
            catch (TrinoException e) {
                log.warn(e, "Failed to drop storage table '%s.%s' for materialized view '%s'", storageSchema, storageTableName, viewName);
            }
        }
        metastore.dropTable(viewName.getSchemaName(), viewName.getTableName(), true);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return metastore.getTable(schemaViewName.getSchemaName(), schemaViewName.getTableName())
                .filter(table -> isTrinoMaterializedView(table.getTableType(), table.getParameters()))
                .map(materializedView -> {
                    String storageTable = materializedView.getParameters().get(STORAGE_TABLE);
                    checkState(storageTable != null, "Storage table missing in definition of materialized view " + schemaViewName);
                    String storageSchema = Optional.ofNullable(materializedView.getParameters().get(STORAGE_SCHEMA))
                            .orElse(schemaViewName.getSchemaName());
                    SchemaTableName storageTableName = new SchemaTableName(storageSchema, storageTable);

                    Table icebergTable;
                    try {
                        icebergTable = loadTable(session, storageTableName);
                    }
                    catch (RuntimeException e) {
                        // The materialized view could be removed concurrently. This may manifest in a number of ways, e.g.
                        // - io.trino.spi.connector.TableNotFoundException
                        // - org.apache.iceberg.exceptions.NotFoundException when accessing manifest file
                        // - other failures when reading storage table's metadata files
                        // Retry, as we're catching broadly.
                        metastore.invalidateTable(schemaViewName.getSchemaName(), schemaViewName.getTableName());
                        metastore.invalidateTable(storageSchema, storageTable);
                        throw new MaterializedViewMayBeBeingRemovedException(e);
                    }
                    return getMaterializedViewDefinition(
                            icebergTable,
                            materializedView.getOwner(),
                            materializedView.getViewOriginalText()
                                    .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No view original text: " + schemaViewName)),
                            storageTableName);
                });
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        metastore.renameTable(source.getSchemaName(), source.getTableName(), target.getSchemaName(), target.getTableName());
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.empty();
    }
}
