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
package io.trino.plugin.objectstore;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.plugin.deltalake.CorruptedDeltaLakeTableHandle;
import io.trino.plugin.deltalake.DeltaLakeInsertTableHandle;
import io.trino.plugin.deltalake.DeltaLakeMergeTableHandle;
import io.trino.plugin.deltalake.DeltaLakeOutputTableHandle;
import io.trino.plugin.deltalake.DeltaLakePartitioningHandle;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.procedure.DeltaLakeTableExecuteHandle;
import io.trino.plugin.hive.HiveInsertTableHandle;
import io.trino.plugin.hive.HiveOutputTableHandle;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.hive.HiveTableExecuteHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.iceberg.CorruptedIcebergTableHandle;
import io.trino.plugin.iceberg.IcebergMergeTableHandle;
import io.trino.plugin.iceberg.IcebergPartitioningHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.IcebergWritableTableHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static java.util.Objects.requireNonNull;

public class ObjectStoreMetadata
        implements ConnectorMetadata
{
    private final ConnectorMetadata hiveMetadata;
    private final ConnectorMetadata icebergMetadata;
    private final ConnectorMetadata deltaMetadata;
    private final ConnectorMetadata hudiMetadata;
    private final ObjectStoreTableProperties tableProperties;
    private final ObjectStoreMaterializedViewProperties materializedViewProperties;
    private final ObjectStoreSessionProperties sessionProperties;
    private final PropertyMetadata<?> hiveFormatProperty;
    private final PropertyMetadata<?> hiveSortedByProperty;
    private final PropertyMetadata<?> icebergFormatProperty;
    private final Procedure flushMetadataCache;
    private final Procedure migrateHiveToIcebergProcedure;
    private final boolean hiveRecursiveDirWalkerEnabled;
    private final TableTypeCache tableTypeCache;

    public ObjectStoreMetadata(
            ConnectorMetadata hiveMetadata,
            ConnectorMetadata icebergMetadata,
            ConnectorMetadata deltaMetadata,
            ConnectorMetadata hudiMetadata,
            ObjectStoreTableProperties tableProperties,
            ObjectStoreMaterializedViewProperties materializedViewProperties,
            ObjectStoreSessionProperties sessionProperties,
            Procedure flushMetadataCache,
            Procedure migrateHiveToIcebergProcedure,
            boolean hiveRecursiveDirWalkerEnabled,
            TableTypeCache tableTypeCache)
    {
        this.hiveMetadata = requireNonNull(hiveMetadata, "hiveMetadata is null");
        this.icebergMetadata = requireNonNull(icebergMetadata, "icebergMetadata is null");
        this.deltaMetadata = requireNonNull(deltaMetadata, "deltaMetadata is null");
        this.hudiMetadata = requireNonNull(hudiMetadata, "hudiMetadata is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.materializedViewProperties = requireNonNull(materializedViewProperties, "materializedViewProperties is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.hiveFormatProperty = tableProperties.getHiveFormatProperty();
        this.hiveSortedByProperty = tableProperties.getHiveSortedByProperty();
        this.icebergFormatProperty = tableProperties.getIcebergFormatProperty();
        this.flushMetadataCache = requireNonNull(flushMetadataCache, "flushMetadataCache is null");
        this.migrateHiveToIcebergProcedure = requireNonNull(migrateHiveToIcebergProcedure, "migrateHiveToIcebergProcedure is null");
        this.hiveRecursiveDirWalkerEnabled = hiveRecursiveDirWalkerEnabled;
        this.tableTypeCache = requireNonNull(tableTypeCache, "tableTypeCache is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return hiveMetadata.schemaExists(unwrap(HIVE, session), schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return hiveMetadata.listSchemaNames(unwrap(HIVE, session));
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        ConnectorTableHandle tableHandle = getTableHandleInOrder(session, tableName, tableTypeCache.getTableTypeAffinity(tableName));
        if (tableHandle != null) {
            tableTypeCache.record(tableName, tableType(tableHandle));
        }
        return tableHandle;
    }

    @Nullable
    private ConnectorTableHandle getTableHandleInOrder(ConnectorSession session, SchemaTableName tableName, List<TableType> candidateTypes)
    {
        checkArgument(candidateTypes.size() == 4);
        TrinoException deferredException = null;
        for (TableType candidateType : candidateTypes) {
            switch (candidateType) {
                case ICEBERG -> {
                    try {
                        return icebergMetadata.getTableHandle(unwrap(ICEBERG, session), tableName, Optional.empty(), Optional.empty());
                    }
                    catch (TrinoException e) {
                        if (!isError(e, UNSUPPORTED_TABLE_TYPE)) {
                            throw e;
                        }
                        deferredException = e;
                    }
                }

                case DELTA -> {
                    try {
                        return deltaMetadata.getTableHandle(unwrap(DELTA, session), tableName);
                    }
                    catch (TrinoException e) {
                        if (!isError(e, UNSUPPORTED_TABLE_TYPE)) {
                            throw e;
                        }
                        deferredException = e;
                    }
                }

                case HUDI -> {
                    try {
                        return hudiMetadata.getTableHandle(unwrap(HUDI, session), tableName);
                    }
                    catch (TrinoException e) {
                        if (!isError(e, UNSUPPORTED_TABLE_TYPE)) {
                            throw e;
                        }
                        deferredException = e;
                    }
                }

                case HIVE -> {
                    try {
                        return hiveMetadata.getTableHandle(unwrap(HIVE, session), tableName);
                    }
                    catch (TrinoException e) {
                        if (!isError(e, UNSUPPORTED_TABLE_TYPE)) {
                            throw e;
                        }
                        deferredException = e;
                    }
                }
            }
        }

        // Propagate last exception
        verify(deferredException != null, "deferredException cannot be null");
        throw deferredException;
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isEmpty() && endVersion.isEmpty()) {
            return getTableHandle(session, tableName);
        }

        try {
            return icebergMetadata.getTableHandle(unwrap(ICEBERG, session), tableName, startVersion, endVersion);
        }
        catch (TrinoException e) {
            if (!isError(e, UNSUPPORTED_TABLE_TYPE)) {
                throw e;
            }
        }
        throw new TrinoException(NOT_SUPPORTED, "Versioning is only supported for Iceberg tables");
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getStatisticsCollectionMetadata(unwrap(tableType, session), tableHandle, analyzeProperties);
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(ConnectorSession session, ConnectorTableHandle tableHandle, String procedureName, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getTableHandleForExecute(unwrap(tableType, session), tableHandle, procedureName, executeProperties, retryMode);
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        TableType tableType = tableType(tableExecuteHandle);
        return delegate(tableType).getLayoutForTableExecute(unwrap(tableType, session), tableExecuteHandle);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle updatedSourceTableHandle)
    {
        TableType tableType = tableType(tableExecuteHandle);
        return delegate(tableType).beginTableExecute(unwrap(tableType, session), tableExecuteHandle, updatedSourceTableHandle);
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        TableType tableType = tableType(tableExecuteHandle);
        delegate(tableType).finishTableExecute(unwrap(tableType, session), tableExecuteHandle, fragments, tableExecuteState);
    }

    @Override
    public void executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        TableType tableType = tableType(tableExecuteHandle);
        delegate(tableType).executeTableExecute(unwrap(tableType, session), tableExecuteHandle);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        // Delta Lake does not have system tables
        return getHiveSystemTable(session, tableName).or(() ->
                getIcebergSystemTable(session, tableName));
    }

    private Optional<SystemTable> getHiveSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return hiveMetadata.getSystemTable(unwrap(HIVE, session), tableName);
        }
        catch (TrinoException e) {
            if (isError(e, HIVE_UNSUPPORTED_FORMAT)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private Optional<SystemTable> getIcebergSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return icebergMetadata.getSystemTable(unwrap(ICEBERG, session), tableName);
        }
        catch (TrinoException e) {
            if (isError(e, UNSUPPORTED_TABLE_TYPE, NOT_SUPPORTED)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return false;
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, RetryMode retryMode)
    {
        if (!tableHandle.getClass().equals(IcebergTableHandle.class)) {
            throw new TrinoException(NOT_SUPPORTED, "Refreshing materialized views with a target table that is not an Iceberg table is not supported");
        }
        return icebergMetadata.beginRefreshMaterializedView(unwrap(ICEBERG, session), tableHandle, sourceTableHandles, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics, List<ConnectorTableHandle> sourceTableHandles)
    {
        return icebergMetadata.finishRefreshMaterializedView(unwrap(ICEBERG, session), tableHandle, insertHandle, fragments, computedStatistics, sourceTableHandles);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        icebergMetadata.createMaterializedView(unwrap(ICEBERG, session), viewName, withProperties(definition, materializedViewProperties.addIcebergPropertyOverrides(definition.getProperties())), replace, ignoreExisting);
        flushMetadataCache(session, viewName);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        ConnectorSession icebergSession = unwrap(ICEBERG, session);
        Optional<ConnectorMaterializedViewDefinition> materializedView = icebergMetadata.getMaterializedView(icebergSession, viewName);
        icebergMetadata.dropMaterializedView(icebergSession, viewName);
        flushMetadataCache(session, viewName);
        materializedView
                .flatMap(ConnectorMaterializedViewDefinition::getStorageTable)
                .map(CatalogSchemaTableName::getSchemaTableName)
                .ifPresent(storageTable -> flushMetadataCache(session, storageTable));
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return icebergMetadata.listMaterializedViews(unwrap(ICEBERG, session), schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return icebergMetadata.getMaterializedViews(unwrap(ICEBERG, session), schemaName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return icebergMetadata.getMaterializedView(unwrap(ICEBERG, session), viewName)
                .map(definition -> withProperties(definition, materializedViewProperties.removeOverriddenOrRemovedIcebergProperties(definition.getProperties())));
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        return icebergMetadata.getMaterializedViewFreshness(unwrap(ICEBERG, session), name);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        icebergMetadata.renameMaterializedView(unwrap(ICEBERG, session), source, target);
        flushMetadataCache(session, source);
        flushMetadataCache(session, target);
    }

    @Override
    public void setMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, Map<String, Optional<Object>> properties)
    {
        icebergMetadata.setMaterializedViewProperties(unwrap(ICEBERG, session), viewName, properties);
        flushMetadataCache(session, viewName);
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).makeCompatiblePartitioning(unwrap(tableType, session), tableHandle, partitioningHandle);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        TableType leftType = tableType(left);
        if (leftType == tableType(right)) {
            return Optional.empty();
        }
        return delegate(leftType).getCommonPartitioningHandle(unwrap(leftType, session), left, right);
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getTableName(unwrap(tableType, session), tableHandle);
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getTableSchema(unwrap(tableType, session), tableHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        ConnectorTableMetadata tableMetadata = delegate(tableType).getTableMetadata(unwrap(tableType, session), tableHandle);
        return withProperties(tableMetadata, ImmutableMap.<String, Object>builder()
                .putAll(wrap(tableType, tableMetadata.getProperties()))
                .put("type", tableType)
                .buildOrThrow());
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        return delegate(tableType(tableHandle)).getInfo(tableHandle);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMetadata.listTables(unwrap(HIVE, session), schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getColumnHandles(unwrap(tableType, session), tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getColumnMetadata(unwrap(tableType, session), tableHandle, columnHandle);
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Map<SchemaTableName, TableColumnsMetadata> tables = new HashMap<>();

        // Hive will include Iceberg tables with the wrong schema, so overwrite later
        hiveMetadata.streamTableColumns(unwrap(HIVE, session), prefix)
                .forEachRemaining(metadata -> tables.put(metadata.getTable(), metadata));

        // Iceberg only lists Iceberg tables
        icebergMetadata.streamTableColumns(unwrap(ICEBERG, session), prefix)
                .forEachRemaining(metadata -> tables.put(metadata.getTable(), metadata));

        // Delta Lake only lists Delta Lake tables
        deltaMetadata.streamTableColumns(unwrap(DELTA, session), prefix)
                .forEachRemaining(metadata -> tables.put(metadata.getTable(), metadata));

        // Hudi lists all tables, so keep the Hive listing

        return tables.values().iterator();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getTableStatistics(unwrap(tableType, session), tableHandle);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        hiveMetadata.createSchema(unwrap(HIVE, session), schemaName, properties, owner);
        flushMetadataCache(session);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        hiveMetadata.dropSchema(unwrap(HIVE, session), schemaName);
        flushMetadataCache(session);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        hiveMetadata.renameSchema(unwrap(HIVE, session), source, target);
        flushMetadataCache(session);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        TableType tableType = tableType(tableMetadata);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Table creation is not supported for Hudi");
        }
        if ((tableType != ICEBERG && tableType != DELTA) && tableMetadata.getColumns().stream().anyMatch(column -> !column.isNullable())) {
            throw new TrinoException(NOT_SUPPORTED, "%s tables do not support NOT NULL columns".formatted(tableType.displayName()));
        }
        tableMetadata = unwrap(tableType, tableMetadata);
        delegate(tableType).createTable(unwrap(tableType, session), tableMetadata, ignoreExisting);
        flushMetadataCache(session, tableMetadata.getTable());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping Hudi tables is not supported");
        }
        delegate(tableType).dropTable(unwrap(tableType, session), tableHandle);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        delegate(tableType).truncateTable(unwrap(tableType, session), tableHandle);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming Hudi tables is not supported");
        }
        delegate(tableType).renameTable(unwrap(tableType, session), tableHandle, newTableName);
        flushMetadataCache(session, tableName(tableHandle));
        flushMetadataCache(session, newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        TableType tableType = tableType(tableHandle);
        if (properties.containsKey("type")) {
            if (properties.size() > 1) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot set table properties when changing table type");
            }
            TableType newTableType = (TableType) properties.get("type")
                    .orElseThrow(() -> new IllegalArgumentException("The type property cannot be empty"));
            migrateTable(session, tableHandle, newTableType);
            flushMetadataCache(session, tableName(tableHandle));
            return;
        }

        Map<String, Object> nullableProperties = properties.entrySet().stream()
                .collect(HashMap::new, (accumulator, entry) -> accumulator.put(entry.getKey(), entry.getValue().orElse(null)), HashMap::putAll);
        nullableProperties = unwrap(tableType, nullableProperties);
        properties = nullableProperties.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> Optional.ofNullable(entry.getValue())));
        delegate(tableType).setTableProperties(unwrap(tableType, session), tableHandle, properties);
        flushMetadataCache(session, tableName(tableHandle));
    }

    private void migrateTable(ConnectorSession session, ConnectorTableHandle tableHandle, TableType newTableType)
    {
        TableType sourceTableType = tableType(tableHandle);
        if (sourceTableType == HIVE && newTableType == ICEBERG) {
            HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
            try {
                String recursiveDirectory = hiveRecursiveDirWalkerEnabled ? "TRUE" : "FALSE";
                migrateHiveToIcebergProcedure.getMethodHandle().invoke(unwrap(ICEBERG, session), hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), recursiveDirectory);
                return;
            }
            catch (Throwable e) {
                throw new TrinoException(NOT_SUPPORTED, "Failed to migrate table", e);
            }
        }
        throw new TrinoException(NOT_SUPPORTED, "Changing table type from '%s' to '%s' is not supported".formatted(sourceTableType, newTableType));
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Setting comments for Hudi tables is not supported");
        }
        delegate(tableType).setTableComment(unwrap(tableType, session), tableHandle, comment);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Setting column comments in Hudi tables is not supported");
        }
        delegate(tableType).setColumnComment(unwrap(tableType, session), tableHandle, column, comment);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Adding columns to Hudi tables is not supported");
        }
        if ((tableType != ICEBERG && tableType != DELTA) && !column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "%s tables do not support NOT NULL columns".formatted(tableType.displayName()));
        }
        delegate(tableType).addColumn(unwrap(tableType, session), tableHandle, column);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        TableType tableType = tableType(tableHandle);
        try {
            delegate(tableType).setColumnType(unwrap(tableType, session), tableHandle, column, type);
        }
        catch (TrinoException e) {
            if (isError(e, NOT_SUPPORTED) && "This connector does not support setting column types".equals(e.getMessage())) {
                throw new TrinoException(NOT_SUPPORTED, "Adding columns to %s tables is not supported".formatted(tableType.displayName()), e);
            }
            throw e;
        }
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming columns in Hudi tables is not supported");
        }
        delegate(tableType).renameColumn(unwrap(tableType, session), tableHandle, source, target);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping columns from Hudi tables is not supported");
        }
        delegate(tableType).dropColumn(unwrap(tableType, session), tableHandle, column);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        TableType tableType = tableType(tableHandle);
        switch (tableType) {
            case HIVE, DELTA, HUDI -> throw new TrinoException(NOT_SUPPORTED, "Dropping fields from %s tables is not supported".formatted(tableType.displayName()));
            default -> {
                // handled below
            }
        }
        delegate(tableType).dropField(unwrap(tableType, session), tableHandle, column, fieldPath);
        flushMetadataCache(session, tableName(tableHandle));
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        TableType tableType = tableType(tableMetadata);
        tableMetadata = unwrap(tableType, tableMetadata);
        return delegate(tableType).getNewTableLayout(unwrap(tableType, session), tableMetadata);
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getInsertLayout(unwrap(tableType, session), tableHandle);
    }

    @Override
    public boolean supportsReportingWrittenBytes(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).supportsReportingWrittenBytes(unwrap(tableType, session), tableHandle);
    }

    @Override
    public boolean supportsReportingWrittenBytes(ConnectorSession session, SchemaTableName schemaTableName, Map<String, Object> tableProperties)
    {
        // This is called for a new table
        TableType tableType = tableType(tableProperties);
        return delegate(tableType).supportsReportingWrittenBytes(unwrap(tableType, session), schemaTableName, tableProperties);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        TableType tableType = tableType(tableMetadata);
        tableMetadata = unwrap(tableType, tableMetadata);
        return delegate(tableType).getStatisticsCollectionMetadataForWrite(unwrap(tableType, session), tableMetadata);
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).beginStatisticsCollection(unwrap(tableType, session), tableHandle);
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        TableType tableType = tableType(tableHandle);
        delegate(tableType).finishStatisticsCollection(unwrap(tableType, session), tableHandle, computedStatistics);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        TableType tableType = tableType(tableMetadata);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Table creation is not supported for Hudi");
        }
        tableMetadata = unwrap(tableType, tableMetadata);
        return delegate(tableType).beginCreateTable(unwrap(tableType, session), tableMetadata, layout, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle outputHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        TableType tableType = tableType(outputHandle);
        Optional<ConnectorOutputMetadata> outputMetadata = delegate(tableType).finishCreateTable(unwrap(tableType, session), outputHandle, fragments, computedStatistics);
        flushMetadataCache(session, tableName(outputHandle));
        return outputMetadata;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Writes are not supported for Hudi tables");
        }
        return delegate(tableType).beginInsert(unwrap(tableType, session), tableHandle, columns, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        TableType tableType = tableType(insertHandle);
        return delegate(tableType).finishInsert(unwrap(tableType, session), insertHandle, fragments, computedStatistics);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        try {
            return delegate(tableType).getRowChangeParadigm(unwrap(tableType, session), tableHandle);
        }
        catch (TrinoException e) {
            if (isError(e, NOT_SUPPORTED) && MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE.equals(e.getMessage())) {
                throw new TrinoException(NOT_SUPPORTED, "Row-level modifications are not supported for Hive tables");
            }
            throw e;
        }
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Writes are not supported for Hudi tables");
        }
        return delegate(tableType).getMergeRowIdColumnHandle(unwrap(tableType, session), tableHandle);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getUpdateLayout(unwrap(tableType, session), tableHandle);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        TableType tableType = tableType(tableHandle);
        try {
            return delegate(tableType).beginMerge(unwrap(tableType, session), tableHandle, retryMode);
        }
        catch (TrinoException e) {
            if (isError(e, NOT_SUPPORTED) && MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE.equals(e.getMessage())) {
                throw new TrinoException(NOT_SUPPORTED, "Modifying Hive table rows is constrained to deletes of whole partitions");
            }
            throw e;
        }
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        TableType tableType = tableType(tableHandle);
        delegate(tableType).finishMerge(unwrap(tableType, session), tableHandle, fragments, computedStatistics);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        hiveMetadata.createView(unwrap(HIVE, session), viewName, definition, replace);
        flushMetadataCache(session, viewName);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        hiveMetadata.renameView(unwrap(HIVE, session), source, target);
        flushMetadataCache(session, source);
        flushMetadataCache(session, target);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        hiveMetadata.dropView(unwrap(HIVE, session), viewName);
        flushMetadataCache(session, viewName);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        hiveMetadata.setViewComment(unwrap(HIVE, session), viewName, comment);
        flushMetadataCache(session, viewName);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        hiveMetadata.setViewColumnComment(unwrap(HIVE, session), viewName, columnName, comment);
        flushMetadataCache(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        // stop gap solution to deal with shared hive metastore between iceberg and hive
        // iceberg stores materialized view with type virtual view and hive lists them without filtering with properties
        Set<SchemaTableName> materializedViews = icebergMetadata.listMaterializedViews(unwrap(ICEBERG, session), schemaName).stream().collect(toImmutableSet());
        return hiveMetadata.listViews(session, schemaName)
                .stream()
                .filter(view -> !materializedViews.contains(view))
                .collect(toImmutableList());
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMetadata.getViews(unwrap(HIVE, session), schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return hiveMetadata.getView(unwrap(HIVE, session), viewName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        return hiveMetadata.getSchemaProperties(unwrap(HIVE, session), schemaName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyDelete(unwrap(tableType, session), tableHandle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).executeDelete(unwrap(tableType, session), tableHandle);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).getTableProperties(unwrap(tableType, session), tableHandle);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle tableHandle, long limit)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyLimit(unwrap(tableType, session), tableHandle, limit);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyFilter(unwrap(tableType, session), tableHandle, constraint);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyProjection(unwrap(tableType, session), tableHandle, projections, assignments);
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle tableHandle, SampleType sampleType, double sampleRatio)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applySample(unwrap(tableType, session), tableHandle, sampleType, sampleRatio);
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session, ConnectorTableHandle tableHandle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyAggregation(unwrap(tableType, session), tableHandle, aggregates, assignments, groupingSets);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        return delegate(tableType).applyTableScanRedirect(unwrap(tableType, session), tableHandle);
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TableType tableType = tableType(tableHandle);
        delegate(tableType).validateScan(unwrap(tableType, session), tableHandle);
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        // only implemented by the Hive connector
        hiveMetadata.beginQuery(unwrap(HIVE, session));
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        // only implemented by the Hive connector
        hiveMetadata.cleanupQuery(unwrap(HIVE, session));
    }

    @Override
    public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle)
    {
        return delegate(tableType(tableHandle)).getCacheTableId(tableHandle);
    }

    @Override
    public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return delegate(tableType(tableHandle)).getCacheColumnId(tableHandle, columnHandle);
    }

    private void flushMetadataCache(ConnectorSession session)
    {
        try {
            flushMetadataCache.getMethodHandle().invoke(session, null, null, null, null, null, null);
        }
        catch (TrinoException | Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to flush metadata cache: " + firstNonNull(e.toString(), e), e);
        }
    }

    private void flushMetadataCache(ConnectorSession session, SchemaTableName table)
    {
        try {
            flushMetadataCache.getMethodHandle().invoke(session, table.getSchemaName(), table.getTableName(), null, null, null, null);
        }
        catch (TrinoException | Error e) {
            throw e;
        }
        catch (Throwable e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to flush metadata cache: " + firstNonNull(e.toString(), e), e);
        }
    }

    private ConnectorMetadata delegate(TableType tableType)
    {
        return switch (tableType) {
            case HIVE -> hiveMetadata;
            case ICEBERG -> icebergMetadata;
            case DELTA -> deltaMetadata;
            case HUDI -> hudiMetadata;
        };
    }

    private static TableType tableType(ConnectorTableHandle handle)
    {
        if (handle instanceof HiveTableHandle) {
            return HIVE;
        }
        if (handle instanceof IcebergTableHandle || handle instanceof CorruptedIcebergTableHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakeTableHandle || handle instanceof CorruptedDeltaLakeTableHandle) {
            return DELTA;
        }
        if (handle instanceof HudiTableHandle) {
            return HUDI;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private static SchemaTableName tableName(ConnectorTableHandle handle)
    {
        if (handle instanceof HiveTableHandle hiveTableHandle) {
            return hiveTableHandle.getSchemaTableName();
        }
        if (handle instanceof IcebergTableHandle icebergTableHandle) {
            return icebergTableHandle.getSchemaTableName();
        }
        if (handle instanceof CorruptedIcebergTableHandle corruptedIcebergTableHandle) {
            return corruptedIcebergTableHandle.schemaTableName();
        }
        if (handle instanceof DeltaLakeTableHandle deltaLakeTableHandle) {
            return deltaLakeTableHandle.getSchemaTableName();
        }
        if (handle instanceof CorruptedDeltaLakeTableHandle corruptedDeltaLakeTableHandle) {
            return corruptedDeltaLakeTableHandle.schemaTableName();
        }
        if (handle instanceof HudiTableHandle hudiTableHandle) {
            return hudiTableHandle.getSchemaTableName();
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private static TableType tableType(ConnectorInsertTableHandle handle)
    {
        if (handle instanceof HiveInsertTableHandle) {
            return HIVE;
        }
        if (handle instanceof IcebergWritableTableHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakeInsertTableHandle) {
            return DELTA;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private static TableType tableType(ConnectorOutputTableHandle handle)
    {
        if (handle instanceof HiveOutputTableHandle) {
            return HIVE;
        }
        if (handle instanceof IcebergWritableTableHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakeOutputTableHandle) {
            return DELTA;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private SchemaTableName tableName(ConnectorOutputTableHandle handle)
    {
        if (handle instanceof HiveOutputTableHandle hiveOutputTableHandle) {
            return hiveOutputTableHandle.getSchemaTableName();
        }
        if (handle instanceof IcebergWritableTableHandle icebergWritableTableHandle) {
            return icebergWritableTableHandle.getName();
        }
        if (handle instanceof DeltaLakeOutputTableHandle deltaLakeOutputTableHandle) {
            return new SchemaTableName(deltaLakeOutputTableHandle.getSchemaName(), deltaLakeOutputTableHandle.getTableName());
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private static TableType tableType(ConnectorMergeTableHandle handle)
    {
        if (handle instanceof IcebergMergeTableHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakeMergeTableHandle) {
            return DELTA;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private TableType tableType(ConnectorTableExecuteHandle handle)
    {
        if (handle instanceof HiveTableExecuteHandle) {
            return HIVE;
        }
        if (handle instanceof IcebergTableExecuteHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakeTableExecuteHandle) {
            return DELTA;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private static TableType tableType(ConnectorPartitioningHandle handle)
    {
        if (handle instanceof HivePartitioningHandle) {
            return HIVE;
        }
        if (handle instanceof IcebergPartitioningHandle) {
            return ICEBERG;
        }
        if (handle instanceof DeltaLakePartitioningHandle) {
            return DELTA;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private static TableType tableType(ConnectorTableMetadata tableMetadata)
    {
        return (TableType) tableMetadata.getProperties().get("type");
    }

    private static TableType tableType(Map<String, Object> tableProperties)
    {
        return (TableType) tableProperties.get("type");
    }

    private Map<String, Object> wrap(TableType tableType, Map<String, Object> tableProperties)
    {
        Map<String, Object> properties = new HashMap<>(tableProperties);
        switch (tableType) {
            case HIVE -> {
                Optional.ofNullable(properties.get("format")).ifPresent(value ->
                        properties.put("format", encodeProperty(hiveFormatProperty, value)));
                Optional.ofNullable(properties.get("sorted_by")).ifPresent(value ->
                        properties.put("sorted_by", encodeProperty(hiveSortedByProperty, value)));
            }
            case ICEBERG -> Optional.ofNullable(properties.get("format")).ifPresent(value ->
                    properties.put("format", encodeProperty(hiveFormatProperty, value)));
            case DELTA, HUDI -> verify(!properties.containsKey("format"), "%s should not have 'format' property".formatted(tableType));
            default -> throw new VerifyException("Unhandled type: " + tableType);
        }
        return properties;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object encodeProperty(PropertyMetadata<?> property, Object value)
    {
        return ((PropertyMetadata) property).encode(value);
    }

    private ConnectorSession unwrap(TableType tableType, ConnectorSession session)
    {
        return sessionProperties.unwrap(tableType, session);
    }

    private ConnectorTableMetadata unwrap(TableType tableType, ConnectorTableMetadata metadata)
    {
        return withProperties(metadata, unwrap(tableType, metadata.getProperties()));
    }

    private Map<String, Object> unwrap(TableType tableType, Map<String, Object> tableProperties)
    {
        Map<String, Object> properties = new HashMap<>(tableProperties);
        switch (tableType) {
            case HIVE -> {
                Optional.ofNullable(properties.get("format")).ifPresent(value ->
                        properties.put("format", decodeProperty(hiveFormatProperty, value)));
                Optional.ofNullable(properties.get("sorted_by")).ifPresent(value ->
                        properties.put("sorted_by", decodeProperty(hiveSortedByProperty, value)));
            }
            case ICEBERG -> Optional.ofNullable(properties.get("format")).ifPresent(value ->
                    properties.put("format", decodeProperty(icebergFormatProperty, value)));
            case DELTA, HUDI -> { /* ignore 'format' property */ }
            default -> throw new VerifyException("Unhandled type: " + tableType);
        }
        properties.entrySet().removeIf(entry ->
                !this.tableProperties.validProperty(tableType, entry.getKey(), entry.getValue()));
        return properties;
    }

    private static <T> T decodeProperty(PropertyMetadata<T> property, Object value)
    {
        try {
            return property.decode(value);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "%s is invalid: %s".formatted(property.getName(), value), e);
        }
    }

    private static boolean isError(TrinoException e, ErrorCodeSupplier... errorCodes)
    {
        return Stream.of(errorCodes)
                .map(ErrorCodeSupplier::toErrorCode)
                .anyMatch(e.getErrorCode()::equals);
    }

    private static ConnectorTableMetadata withProperties(ConnectorTableMetadata metadata, Map<String, Object> properties)
    {
        return new ConnectorTableMetadata(metadata.getTable(), metadata.getColumns(), properties, metadata.getComment());
    }

    private static ConnectorMaterializedViewDefinition withProperties(ConnectorMaterializedViewDefinition definition, Map<String, Object> properties)
    {
        return new ConnectorMaterializedViewDefinition(
                definition.getOriginalSql(),
                definition.getStorageTable(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns(),
                definition.getGracePeriod(),
                definition.getComment(),
                definition.getOwner(),
                properties);
    }
}
