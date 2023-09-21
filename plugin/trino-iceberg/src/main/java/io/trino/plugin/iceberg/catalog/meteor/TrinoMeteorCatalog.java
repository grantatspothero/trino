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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.util.MaybeLazy;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.plugin.iceberg.TrinoMetricsReporter.TRINO_METRICS_REPORTER;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class TrinoMeteorCatalog
        implements TrinoCatalog
{
    private final MeteorCatalogClient apiClient;
    private final CatalogHandle catalogHandle;
    private final IcebergTableOperationsProvider tableOperationsProvider;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public TrinoMeteorCatalog(
            MeteorCatalogClient apiClient,
            CatalogHandle catalogHandle,
            IcebergTableOperationsProvider tableOperationsProvider)
    {
        this.apiClient = requireNonNull(apiClient, "apiClient is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        if (!isGalaxyQuery(session)) {
            throw new TrinoException(NOT_SUPPORTED, "Only Trino queries from Starburst Galaxy is supported for this catalog");
        }

        return apiClient.namespaceExists(session.getIdentity(), catalogHandle.getVersion().toString(), namespace);
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        if (!isGalaxyQuery(session)) {
            throw new TrinoException(NOT_SUPPORTED, "Only Trino queries from Starburst Galaxy is supported for this catalog");
        }

        return apiClient.listNamespaces(session.getIdentity(), catalogHandle.getVersion().toString())
                .stream()
                .collect(toImmutableList());
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        // the REST specification currently does not have a way of defining ownership
        return Optional.empty();
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        if (!isGalaxyQuery(session)) {
            throw new TrinoException(NOT_SUPPORTED, "Only Trino queries from Starburst Galaxy is supported for this catalog");
        }

        return apiClient.listTables(session.getIdentity(), catalogHandle.getVersion().toString(), namespace);
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> namespace)
    {
        // views and materialized views are currently not supported
        verify(listViews(session, namespace).isEmpty(), "Unexpected views support");
        verify(listMaterializedViews(session, namespace).isEmpty(), "Unexpected views support");
        return listTables(session, namespace).stream()
                .collect(toImmutableMap(identity(), ignore -> RelationType.TABLE));
    }

    @Override
    public Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(ConnectorSession session, Optional<String> namespace, UnaryOperator<Set<SchemaTableName>> relationFilter, Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Optional<Iterator<RelationCommentMetadata>> streamRelationComments(ConnectorSession session, Optional<String> namespace, UnaryOperator<Set<SchemaTableName>> relationFilter, Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public Transaction newCreateOrReplaceTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        // Since it is currently not possible to obtain the table location, even if we drop the table from the metastore,
        // it is still impossible to delete the table location.
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableMetadata metadata = tableMetadataCache.computeIfAbsent(
                schemaTableName,
                ignore -> {
                    TableOperations operations = tableOperationsProvider.createTableOperations(
                            this,
                            session,
                            schemaTableName.getSchemaName(),
                            schemaTableName.getTableName(),
                            Optional.empty(),
                            Optional.empty());
                    return new BaseTable(operations, quotedTableName(schemaTableName), TRINO_METRICS_REPORTER).operations().current();
                });

        return getIcebergTableWithMetadata(
                this,
                tableOperationsProvider,
                session,
                schemaTableName,
                metadata);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return ImmutableMap.of();
    }

    @Override
    public MaybeLazy<List<ColumnMetadata>> getTableColumnMetadata(ConnectorSession session, io.trino.plugin.hive.metastore.Table metastoreTable)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public MaybeLazy<Optional<String>> getTableComment(ConnectorSession session, io.trino.plugin.hive.metastore.Table metastoreTable)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableList.of();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableMap.of();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableList.of();
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties,
            boolean replace,
            boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        return ImmutableMap.of();
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName)
    {
        return Optional.empty();
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    @Override
    public void updateMaterializedViewRefreshSchedule(ConnectorSession session, SchemaTableName viewName, Optional<String> schedule)
    {
        throw new TrinoException(NOT_SUPPORTED, "Operation not supported by Meteor Catalog");
    }

    public MeteorCatalogClient getCatalogClient()
    {
        return apiClient;
    }

    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    private boolean isGalaxyQuery(ConnectorSession session)
    {
        return session.getIdentity().getExtraCredentials().containsKey("accountId");
    }
}
