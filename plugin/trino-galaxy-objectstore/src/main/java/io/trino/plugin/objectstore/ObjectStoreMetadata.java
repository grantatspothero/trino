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
import io.trino.plugin.deltalake.DeltaLakeInsertTableHandle;
import io.trino.plugin.deltalake.DeltaLakeMergeTableHandle;
import io.trino.plugin.deltalake.DeltaLakeOutputTableHandle;
import io.trino.plugin.deltalake.DeltaLakePartitioningHandle;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.procedure.DeltaLakeTableExecuteHandle;
import io.trino.plugin.hive.HiveInsertTableHandle;
import io.trino.plugin.hive.HiveOutputTableHandle;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveTableExecuteHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergMergeTableHandle;
import io.trino.plugin.iceberg.IcebergPartitioningHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.IcebergWritableTableHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaName;
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

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNKNOWN_TABLE_TYPE;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static java.util.Objects.requireNonNull;

public class ObjectStoreMetadata
        implements ConnectorMetadata
{
    private static final PropertyMetadata<HiveStorageFormat> HIVE_FORMAT = enumProperty("format", "", HiveStorageFormat.class, null, false);
    private static final PropertyMetadata<IcebergFileFormat> ICEBERG_FORMAT = enumProperty("format", "", IcebergFileFormat.class, null, false);

    private final ConnectorMetadata hiveMetadata;
    private final ConnectorMetadata icebergMetadata;
    private final ConnectorMetadata deltaMetadata;
    private final ConnectorMetadata hudiMetadata;
    private final ObjectStoreTableProperties tableProperties;
    private final ObjectStoreMaterializedViewProperties materializedViewProperties;

    public ObjectStoreMetadata(
            ConnectorMetadata hiveMetadata,
            ConnectorMetadata icebergMetadata,
            ConnectorMetadata deltaMetadata,
            ConnectorMetadata hudiMetadata,
            ObjectStoreTableProperties tableProperties,
            ObjectStoreMaterializedViewProperties materializedViewProperties)
    {
        this.hiveMetadata = requireNonNull(hiveMetadata, "hiveMetadata is null");
        this.icebergMetadata = requireNonNull(icebergMetadata, "icebergMetadata is null");
        this.deltaMetadata = requireNonNull(deltaMetadata, "deltaMetadata is null");
        this.hudiMetadata = requireNonNull(hudiMetadata, "hudiMetadata is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.materializedViewProperties = requireNonNull(materializedViewProperties, "materializedViewProperties is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return hiveMetadata.schemaExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return hiveMetadata.listSchemaNames(session);
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return icebergMetadata.getTableHandle(session, tableName, Optional.empty(), Optional.empty());
        }
        catch (TrinoException e) {
            if (!isError(e, UNSUPPORTED_TABLE_TYPE)) {
                throw e;
            }
        }

        try {
            return deltaMetadata.getTableHandle(session, tableName);
        }
        catch (TrinoException e) {
            if (!isError(e, UNSUPPORTED_TABLE_TYPE)) {
                throw e;
            }
        }

        try {
            return hudiMetadata.getTableHandle(session, tableName);
        }
        catch (TrinoException e) {
            if (!isError(e, HUDI_UNKNOWN_TABLE_TYPE)) {
                throw e;
            }
        }

        return hiveMetadata.getTableHandle(session, tableName);
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
            return icebergMetadata.getTableHandle(session, tableName, startVersion, endVersion);
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
        return delegate(tableHandle).getStatisticsCollectionMetadata(session, tableHandle, analyzeProperties);
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(ConnectorSession session, ConnectorTableHandle tableHandle, String procedureName, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        return delegate(tableHandle).getTableHandleForExecute(session, tableHandle, procedureName, executeProperties, retryMode);
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        return delegate(tableExecuteHandle).getLayoutForTableExecute(session, tableExecuteHandle);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle updatedSourceTableHandle)
    {
        return delegate(tableExecuteHandle).beginTableExecute(session, tableExecuteHandle, updatedSourceTableHandle);
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        delegate(tableExecuteHandle).finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteState);
    }

    @Override
    public void executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        delegate(tableExecuteHandle).executeTableExecute(session, tableExecuteHandle);
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
            return hiveMetadata.getSystemTable(session, tableName);
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
            return icebergMetadata.getSystemTable(session, tableName);
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
        return icebergMetadata.beginRefreshMaterializedView(session, tableHandle, sourceTableHandles, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics, List<ConnectorTableHandle> sourceTableHandles)
    {
        return icebergMetadata.finishRefreshMaterializedView(session, tableHandle, insertHandle, fragments, computedStatistics, sourceTableHandles);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        icebergMetadata.createMaterializedView(session, viewName, withProperties(definition, materializedViewProperties.addIcebergPropertyOverrides(definition.getProperties())), replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        icebergMetadata.dropMaterializedView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return icebergMetadata.listMaterializedViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return icebergMetadata.getMaterializedViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return icebergMetadata.getMaterializedView(session, viewName)
                .map(definition -> withProperties(definition, materializedViewProperties.removeOverriddenOrRemovedIcebergProperties(definition.getProperties())));
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        return icebergMetadata.getMaterializedViewFreshness(session, name);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        icebergMetadata.renameMaterializedView(session, source, target);
    }

    @Override
    public void setMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, Map<String, Optional<Object>> properties)
    {
        icebergMetadata.setMaterializedViewProperties(session, viewName, properties);
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        return delegate(tableHandle).makeCompatiblePartitioning(session, tableHandle, partitioningHandle);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        ConnectorMetadata metadata = delegate(left);
        if (!isSameConnector(metadata, delegate(right))) {
            return Optional.empty();
        }
        return metadata.getCommonPartitioningHandle(session, left, right);
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).getTableSchema(session, tableHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorMetadata metadata = delegate(tableHandle);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        return withProperties(tableMetadata, ImmutableMap.<String, Object>builder()
                .putAll(wrap(tableType(metadata), tableMetadata.getProperties()))
                .put("type", tableType(metadata))
                .buildOrThrow());
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).getInfo(tableHandle);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMetadata.listTables(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).getColumnHandles(session, tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return delegate(tableHandle).getColumnMetadata(session, tableHandle, columnHandle);
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Map<SchemaTableName, TableColumnsMetadata> tables = new HashMap<>();

        // Hive will include Iceberg tables with the wrong schema, so overwrite later
        hiveMetadata.streamTableColumns(session, prefix)
                .forEachRemaining(metadata -> tables.put(metadata.getTable(), metadata));

        // Iceberg only lists Iceberg tables
        icebergMetadata.streamTableColumns(session, prefix)
                .forEachRemaining(metadata -> tables.put(metadata.getTable(), metadata));

        // Delta Lake only lists Delta Lake tables
        deltaMetadata.streamTableColumns(session, prefix)
                .forEachRemaining(metadata -> tables.put(metadata.getTable(), metadata));

        // Hudi lists all tables, so keep the Hive listing

        return tables.values().iterator();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).getTableStatistics(session, tableHandle);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        hiveMetadata.createSchema(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        hiveMetadata.dropSchema(session, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        hiveMetadata.renameSchema(session, source, target);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorMetadata metadata = delegate(tableMetadata);
        TableType tableType = tableType(metadata);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Table creation is not supported for Hudi");
        }
        if ((tableType != ICEBERG) && tableMetadata.getColumns().stream().anyMatch(column -> !column.isNullable())) {
            throw new TrinoException(NOT_SUPPORTED, "%s tables do not support NOT NULL columns".formatted(tableType.displayName()));
        }
        tableMetadata = unwrap(tableType, tableMetadata);
        metadata.createTable(session, tableMetadata, ignoreExisting);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorMetadata metadata = delegate(tableHandle);
        if (tableType(metadata) == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping Hudi tables is not supported");
        }
        metadata.dropTable(session, tableHandle);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        delegate(tableHandle).truncateTable(session, tableHandle);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        ConnectorMetadata metadata = delegate(tableHandle);
        if (tableType(metadata) == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming Hudi tables is not supported");
        }
        metadata.renameTable(session, tableHandle, newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        delegate(tableHandle).setTableProperties(session, tableHandle, properties);
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        ConnectorMetadata metadata = delegate(tableHandle);
        if (tableType(metadata) == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Setting comments for Hudi tables is not supported");
        }
        metadata.setTableComment(session, tableHandle, comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        ConnectorMetadata metadata = delegate(tableHandle);
        if (tableType(metadata) == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Setting column comments in Hudi tables is not supported");
        }
        metadata.setColumnComment(session, tableHandle, column, comment);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        ConnectorMetadata delegate = delegate(tableHandle);
        TableType tableType = tableType(delegate);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Adding columns to Hudi tables is not supported");
        }
        if ((tableType != ICEBERG) && !column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "%s tables do not support NOT NULL columns".formatted(tableType.displayName()));
        }
        delegate.addColumn(session, tableHandle, column);
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        ConnectorMetadata delegate = delegate(tableHandle);
        TableType tableType = tableType(delegate);
        try {
            delegate.setColumnType(session, tableHandle, column, type);
        }
        catch (TrinoException e) {
            if (isError(e, NOT_SUPPORTED) && "This connector does not support setting column types".equals(e.getMessage())) {
                throw new TrinoException(NOT_SUPPORTED, "Adding columns to %s tables is not supported".formatted(tableType.displayName()), e);
            }
            throw e;
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        ConnectorMetadata metadata = delegate(tableHandle);
        if (tableType(metadata) == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming columns in Hudi tables is not supported");
        }
        metadata.renameColumn(session, tableHandle, source, target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        ConnectorMetadata metadata = delegate(tableHandle);
        if (tableType(metadata) == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping columns from Hudi tables is not supported");
        }
        metadata.dropColumn(session, tableHandle, column);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        ConnectorMetadata metadata = delegate(tableMetadata);
        tableMetadata = unwrap(tableType(metadata), tableMetadata);
        return metadata.getNewTableLayout(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).getInsertLayout(session, tableHandle);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        ConnectorMetadata metadata = delegate(tableMetadata);
        tableMetadata = unwrap(tableType(metadata), tableMetadata);
        return metadata.getStatisticsCollectionMetadataForWrite(session, tableMetadata);
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).beginStatisticsCollection(session, tableHandle);
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        delegate(tableHandle).finishStatisticsCollection(session, tableHandle, computedStatistics);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        ConnectorMetadata metadata = delegate(tableMetadata);
        TableType tableType = tableType(metadata);
        if (tableType == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Table creation is not supported for Hudi");
        }
        tableMetadata = unwrap(tableType, tableMetadata);
        return metadata.beginCreateTable(session, tableMetadata, layout, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle outputHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return delegate(outputHandle).finishCreateTable(session, outputHandle, fragments, computedStatistics);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        ConnectorMetadata metadata = delegate(tableHandle);
        if (tableType(metadata) == HUDI) {
            throw new TrinoException(NOT_SUPPORTED, "Writes are not supported for Hudi tables");
        }
        return metadata.beginInsert(session, tableHandle, columns, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return delegate(insertHandle).finishInsert(session, insertHandle, fragments, computedStatistics);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            return delegate(tableHandle).getRowChangeParadigm(session, tableHandle);
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
        ConnectorMetadata metadata = delegate(tableHandle);
        return switch (tableType(metadata)) {
            case HIVE -> metadata.getMergeRowIdColumnHandle(session, tableHandle);
            case HUDI -> throw new TrinoException(NOT_SUPPORTED, "Writes are not supported for Hudi tables");
            default -> metadata.getMergeRowIdColumnHandle(session, tableHandle);
        };
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).getUpdateLayout(session, tableHandle);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        try {
            return delegate(tableHandle).beginMerge(session, tableHandle, retryMode);
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
        delegate(tableHandle).finishMerge(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        hiveMetadata.createView(session, viewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        hiveMetadata.renameView(session, source, target);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        hiveMetadata.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        // stop gap solution to deal with shared hive metastore between iceberg and hive
        // iceberg stores materialized view with type virtual view and hive lists them without filtering with properties
        Set<SchemaTableName> materializedViews = icebergMetadata.listMaterializedViews(session, schemaName).stream().collect(toImmutableSet());
        return hiveMetadata.listViews(session, schemaName)
                .stream()
                .filter(view -> !materializedViews.contains(view))
                .collect(toImmutableList());
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMetadata.getViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return hiveMetadata.getView(session, viewName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return hiveMetadata.getSchemaProperties(session, schemaName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).applyDelete(session, tableHandle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).executeDelete(session, tableHandle);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).getTableProperties(session, tableHandle);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle tableHandle, long limit)
    {
        return delegate(tableHandle).applyLimit(session, tableHandle, limit);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        return delegate(tableHandle).applyFilter(session, tableHandle, constraint);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return delegate(tableHandle).applyProjection(session, tableHandle, projections, assignments);
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle tableHandle, SampleType sampleType, double sampleRatio)
    {
        return delegate(tableHandle).applySample(session, tableHandle, sampleType, sampleRatio);
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session, ConnectorTableHandle tableHandle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return delegate(tableHandle).applyAggregation(session, tableHandle, aggregates, assignments, groupingSets);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate(tableHandle).applyTableScanRedirect(session, tableHandle);
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        delegate(tableHandle).validateScan(session, tableHandle);
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        // only implemented by the Hive connector
        hiveMetadata.beginQuery(session);
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        // only implemented by the Hive connector
        hiveMetadata.cleanupQuery(session);
    }

    @SuppressWarnings("ObjectEquality")
    private TableType tableType(ConnectorMetadata metadata)
    {
        if (metadata == hiveMetadata) {
            return HIVE;
        }
        if (metadata == icebergMetadata) {
            return ICEBERG;
        }
        if (metadata == deltaMetadata) {
            return DELTA;
        }
        if (metadata == hudiMetadata) {
            return HUDI;
        }
        throw new VerifyException("Unknown instance: " + metadata);
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

    private ConnectorMetadata delegate(ConnectorTableHandle handle)
    {
        if (handle instanceof HiveTableHandle) {
            return hiveMetadata;
        }
        if (handle instanceof IcebergTableHandle) {
            return icebergMetadata;
        }
        if (handle instanceof DeltaLakeTableHandle) {
            return deltaMetadata;
        }
        if (handle instanceof HudiTableHandle) {
            return hudiMetadata;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private ConnectorMetadata delegate(ConnectorInsertTableHandle handle)
    {
        if (handle instanceof HiveInsertTableHandle) {
            return hiveMetadata;
        }
        if (handle instanceof IcebergWritableTableHandle) {
            return icebergMetadata;
        }
        if (handle instanceof DeltaLakeInsertTableHandle) {
            return deltaMetadata;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private ConnectorMetadata delegate(ConnectorOutputTableHandle handle)
    {
        if (handle instanceof HiveOutputTableHandle) {
            return hiveMetadata;
        }
        if (handle instanceof IcebergWritableTableHandle) {
            return icebergMetadata;
        }
        if (handle instanceof DeltaLakeOutputTableHandle) {
            return deltaMetadata;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private ConnectorMetadata delegate(ConnectorMergeTableHandle handle)
    {
        if (handle instanceof IcebergMergeTableHandle) {
            return icebergMetadata;
        }
        if (handle instanceof DeltaLakeMergeTableHandle) {
            return deltaMetadata;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private ConnectorMetadata delegate(ConnectorTableExecuteHandle handle)
    {
        if (handle instanceof HiveTableExecuteHandle) {
            return hiveMetadata;
        }
        if (handle instanceof IcebergTableExecuteHandle) {
            return icebergMetadata;
        }
        if (handle instanceof DeltaLakeTableExecuteHandle) {
            return deltaMetadata;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private ConnectorMetadata delegate(ConnectorPartitioningHandle handle)
    {
        if (handle instanceof HivePartitioningHandle) {
            return hiveMetadata;
        }
        if (handle instanceof IcebergPartitioningHandle) {
            return icebergMetadata;
        }
        if (handle instanceof DeltaLakePartitioningHandle) {
            return deltaMetadata;
        }
        throw new VerifyException("Unhandled class: " + handle.getClass().getName());
    }

    private ConnectorMetadata delegate(ConnectorTableMetadata tableMetadata)
    {
        return delegate(tableMetadata.getProperties());
    }

    private ConnectorMetadata delegate(Map<String, Object> properties)
    {
        return delegate((TableType) properties.get("type"));
    }

    private static Map<String, Object> wrap(TableType tableType, Map<String, Object> tableProperties)
    {
        Map<String, Object> properties = new HashMap<>(tableProperties);
        switch (tableType) {
            case HIVE -> Optional.ofNullable(properties.get("format")).ifPresent(value ->
                    properties.put("format", ((HiveStorageFormat) value).name()));
            case ICEBERG -> Optional.ofNullable(properties.get("format")).ifPresent(value ->
                    properties.put("format", ((IcebergFileFormat) value).name()));
            case DELTA, HUDI -> verify(!properties.containsKey("format"), "%s should not have 'format' property".formatted(tableType));
            default -> throw new VerifyException("Unhandled type: " + tableType);
        }
        return properties;
    }

    private ConnectorTableMetadata unwrap(TableType tableType, ConnectorTableMetadata metadata)
    {
        return withProperties(metadata, unwrap(tableType, metadata.getProperties()));
    }

    private Map<String, Object> unwrap(TableType tableType, Map<String, Object> tableProperties)
    {
        Map<String, Object> properties = new HashMap<>(tableProperties);
        switch (tableType) {
            case HIVE -> Optional.ofNullable(properties.get("format")).ifPresent(value ->
                    properties.put("format", decodeProperty(HIVE_FORMAT, value)));
            case ICEBERG -> Optional.ofNullable(properties.get("format")).ifPresent(value ->
                    properties.put("format", decodeProperty(ICEBERG_FORMAT, value)));
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

    private static boolean isSameConnector(ConnectorMetadata left, ConnectorMetadata right)
    {
        return left.getClass() == right.getClass();
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
        return new ConnectorMaterializedViewDefinition(definition.getOriginalSql(), definition.getStorageTable(), definition.getCatalog(), definition.getSchema(), definition.getColumns(), definition.getComment(), definition.getOwner(), properties);
    }
}
