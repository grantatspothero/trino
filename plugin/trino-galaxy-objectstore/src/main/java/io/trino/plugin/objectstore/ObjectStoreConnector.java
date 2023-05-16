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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.hive.HiveConnector;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Sets.immutableEnumSet;
import static com.google.common.collect.Sets.symmetricDifference;
import static com.google.common.collect.Streams.forEachPair;
import static io.trino.plugin.objectstore.FeatureExposure.UNDEFINED;
import static io.trino.plugin.objectstore.FeatureExposures.procedureExposureDecisions;
import static io.trino.plugin.objectstore.FeatureExposures.tableProcedureExposureDecisions;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.IGNORE_DESCRIPTION;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.VERIFY_DESCRIPTION;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.verifyPropertyMetadata;
import static io.trino.spi.connector.ConnectorCapabilities.MATERIALIZED_VIEW_GRACE_PERIOD;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class ObjectStoreConnector
        implements Connector
{
    private final Connector hiveConnector;
    private final Connector icebergConnector;
    private final Connector deltaConnector;
    private final Connector hudiConnector;
    private final LifeCycleManager lifeCycleManager;
    private final ObjectStoreSplitManager splitManager;
    private final ObjectStorePageSourceProvider pageSourceProvider;
    private final ObjectStorePageSinkProvider pageSinkProvider;
    private final ObjectStoreNodePartitioningProvider nodePartitioningProvider;
    private final List<PropertyMetadata<?>> schemaProperties;
    private final ObjectStoreTableProperties tableProperties;
    private final List<PropertyMetadata<?>> columnProperties;
    private final ObjectStoreMaterializedViewProperties materializedViewProperties;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> analyzeProperties;
    private final Set<Procedure> procedures;
    private final Set<TableProcedureMetadata> tableProcedures;
    private final Procedure flushMetadataCache;
    private final Procedure migrateHiveToIcebergProcedure;
    private final boolean hiveRecursiveDirWalkerEnabled;

    private final TableTypeCache tableTypeCache = new TableTypeCache();

    @Inject
    public ObjectStoreConnector(
            DelegateConnectors delegates,
            LifeCycleManager lifeCycleManager,
            ObjectStoreSplitManager splitManager,
            ObjectStorePageSourceProvider pageSourceProvider,
            ObjectStorePageSinkProvider pageSinkProvider,
            ObjectStoreNodePartitioningProvider nodePartitioningProvider,
            ObjectStoreTableProperties tableProperties,
            ObjectStoreMaterializedViewProperties materializedViewProperties,
            Set<Procedure> objectStoreProcedures)
    {
        this.hiveConnector = delegates.hiveConnector();
        this.icebergConnector = delegates.icebergConnector();
        this.deltaConnector = delegates.deltaConnector();
        this.hudiConnector = delegates.hudiConnector();
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.schemaProperties = schemaProperties();
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.columnProperties = columnProperties(delegates);
        this.materializedViewProperties = requireNonNull(materializedViewProperties, "materializedViewProperties is null");
        this.sessionProperties = sessionProperties(delegates);
        this.analyzeProperties = analyzeProperties(delegates);
        this.procedures = procedures(delegates, objectStoreProcedures);
        this.tableProcedures = tableProcedures(delegates);
        this.flushMetadataCache = objectStoreProcedures.stream()
                .filter(procedure -> procedure.getName().equals("flush_metadata_cache"))
                .collect(onlyElement());
        this.migrateHiveToIcebergProcedure = icebergConnector.getProcedures().stream()
                .filter(procedure -> procedure.getName().equals("migrate"))
                .collect(onlyElement());
        this.hiveRecursiveDirWalkerEnabled = ((HiveConnector) hiveConnector).isRecursiveDirWalkerEnabled();
    }

    private static List<PropertyMetadata<?>> schemaProperties()
    {
        return ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        "location",
                        "Base file system location URI",
                        null,
                        false))
                .build();
    }

    private static List<PropertyMetadata<?>> columnProperties(DelegateConnectors delegates)
    {
        verify(delegates.hiveConnector().getColumnProperties().stream().allMatch(
                property -> property.getName().startsWith("partition_projection_")), "Unexpected Hive column properties");
        verify(delegates.icebergConnector().getColumnProperties().isEmpty(), "Unexpected Iceberg column properties");
        verify(delegates.deltaConnector().getColumnProperties().isEmpty(), "Unexpected Delta Lake column properties");
        verify(delegates.hudiConnector().getColumnProperties().isEmpty(), "Unexpected Hudi column properties");
        return ImmutableList.of();
    }

    private static List<PropertyMetadata<?>> sessionProperties(DelegateConnectors delegates)
    {
        Set<String> ignoredDescriptions = ImmutableSet.<String>builder()
                .add("compression_codec")
                .add("projection_pushdown_enabled")
                .add("timestamp_precision")
                .add("minimum_assigned_split_weight")
                .build();

        Map<String, PropertyMetadata<?>> sessionProperties = new HashMap<>();
        for (Connector connector : delegates.asList()) {
            for (PropertyMetadata<?> property : connector.getSessionProperties()) {
                PropertyMetadata<?> existing = sessionProperties.putIfAbsent(property.getName(), property);
                if (existing != null) {
                    verifyPropertyMetadata(
                            property,
                            existing,
                            ignoredDescriptions.contains(property.getName()) ? IGNORE_DESCRIPTION : VERIFY_DESCRIPTION);
                }
            }
        }
        return ImmutableList.copyOf(sessionProperties.values());
    }

    private static List<PropertyMetadata<?>> analyzeProperties(DelegateConnectors delegates)
    {
        Map<String, PropertyMetadata<?>> properties = new HashMap<>();
        for (Connector connector : delegates.asList()) {
            for (PropertyMetadata<?> property : connector.getAnalyzeProperties()) {
                PropertyMetadata<?> existing = properties.putIfAbsent(property.getName(), property);
                if (existing != null) {
                    verifyPropertyMetadata(property, existing);
                }
            }
        }
        return ImmutableList.copyOf(properties.values());
    }

    private static Set<Procedure> procedures(DelegateConnectors delegates, Set<Procedure> objectStoreProcedures)
    {
        Map<String, Procedure> procedures = new HashMap<>();
        objectStoreProcedures.forEach(procedure -> procedures.put(procedure.getName(), procedure));
        Table<TableType, String, FeatureExposure> featureExposures = HashBasedTable.create(procedureExposureDecisions());
        delegates.byType().forEach((type, connector) -> {
            for (Procedure procedure : connector.getProcedures()) {
                String name = procedure.getName();
                switch (firstNonNull(featureExposures.remove(type, name), UNDEFINED)) {
                    case HIDDEN -> { /* skipped */ }
                    case UNDEFINED -> throw new IllegalStateException("Unknown procedure provided by %s: %s".formatted(type, name));
                    case EXPOSED -> {
                        Procedure existing = procedures.putIfAbsent(name, procedure);
                        if (existing != null) {
                            throw new VerifyException("Duplicate procedure: " + name);
                        }
                    }
                }
            }
        });

        if (!featureExposures.isEmpty()) {
            throw new IllegalStateException("Procedures no longer provided: " + Maps.transformValues(featureExposures.rowMap(), Map::keySet));
        }

        return ImmutableSet.copyOf(procedures.values());
    }

    private static Set<TableProcedureMetadata> tableProcedures(DelegateConnectors delegates)
    {
        // Table procedures are currently defined on per-Connector basis. TODO Let engine ask for table procedures via ConnectorMetadata, for given table, so that we don't have to resolve collisions.

        Map<String, TableProcedureMetadata> tableProcedures = new HashMap<>();
        Table<TableType, String, FeatureExposure> featureExposures = HashBasedTable.create(tableProcedureExposureDecisions());
        delegates.byType().forEach((type, connector) -> {
            for (TableProcedureMetadata procedure : connector.getTableProcedures()) {
                String name = procedure.getName();
                verify(name.equals(name.toUpperCase(Locale.ROOT)), "Procedure name is not uppercase: %s", name);

                switch (firstNonNull(featureExposures.remove(type, name), UNDEFINED)) {
                    case HIDDEN -> { /* skipped */ }
                    case UNDEFINED -> throw new IllegalStateException("Unknown table procedure provided by %s: %s".formatted(type, name));
                    case EXPOSED -> {
                        TableProcedureMetadata existing = tableProcedures.putIfAbsent(name, procedure);
                        if (existing == null) {
                            continue;
                        }

                        verify(procedure.getExecutionMode().isReadsData() == existing.getExecutionMode().isReadsData(),
                                "Procedure uses different execution mode for reads data: %s", name);
                        verify(procedure.getExecutionMode().supportsFilter() == existing.getExecutionMode().supportsFilter(),
                                "Procedure uses different execution mode for supports filter: %s", name);

                        Set<String> difference = symmetricDifference(
                                procedure.getProperties().stream()
                                        .map(PropertyMetadata::getName)
                                        .collect(toSet()),
                                existing.getProperties().stream()
                                        .map(PropertyMetadata::getName)
                                        .collect(toSet()));
                        verify(difference.isEmpty(), "Procedure '%s' has different properties: %s", name, difference);

                        forEachPair(
                                procedure.getProperties().stream().sorted(comparing(PropertyMetadata::getName)),
                                existing.getProperties().stream().sorted(comparing(PropertyMetadata::getName)),
                                PropertyMetadataValidation::verifyPropertyMetadata);
                    }
                }
            }
        });

        if (!featureExposures.isEmpty()) {
            throw new IllegalStateException("Procedures no longer provided: " + Maps.transformValues(featureExposures.rowMap(), Map::keySet));
        }

        return ImmutableSet.copyOf(tableProcedures.values());
    }

    @Override
    public ObjectStoreTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);

        HiveTransactionHandle hiveHandle = (HiveTransactionHandle) hiveConnector.beginTransaction(isolationLevel, readOnly, true);
        HiveTransactionHandle icebergHandle = (HiveTransactionHandle) icebergConnector.beginTransaction(isolationLevel, readOnly, true);
        HiveTransactionHandle deltaHandle = (HiveTransactionHandle) deltaConnector.beginTransaction(isolationLevel, readOnly, true);
        HiveTransactionHandle hudiHandle = (HiveTransactionHandle) hudiConnector.beginTransaction(isolationLevel, readOnly, true);

        return new ObjectStoreTransactionHandle(hiveHandle, icebergHandle, deltaHandle, hudiHandle);
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        ObjectStoreTransactionHandle handle = (ObjectStoreTransactionHandle) transactionHandle;

        // only one of the connectors will be used
        hiveConnector.commit(handle.getHiveHandle());
        icebergConnector.commit(handle.getIcebergHandle());
        deltaConnector.commit(handle.getDeltaHandle());
        hudiConnector.commit(handle.getHudiHandle());
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        ObjectStoreTransactionHandle handle = (ObjectStoreTransactionHandle) transactionHandle;

        // only one of the connectors will be used
        hiveConnector.rollback(handle.getHiveHandle());
        icebergConnector.rollback(handle.getIcebergHandle());
        deltaConnector.rollback(handle.getDeltaHandle());
        hudiConnector.rollback(handle.getHudiHandle());
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
        hiveConnector.shutdown();
        icebergConnector.shutdown();
        deltaConnector.shutdown();
        hudiConnector.shutdown();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        ObjectStoreTransactionHandle handle = (ObjectStoreTransactionHandle) transactionHandle;

        ConnectorMetadata hiveMetadata = hiveConnector.getMetadata(session, handle.getHiveHandle());
        ConnectorMetadata icebergMetadata = icebergConnector.getMetadata(session, handle.getIcebergHandle());
        ConnectorMetadata deltaMetadata = deltaConnector.getMetadata(session, handle.getDeltaHandle());
        ConnectorMetadata hudiMetadata = hudiConnector.getMetadata(session, handle.getHudiHandle());

        return new ObjectStoreMetadata(
                hiveMetadata,
                icebergMetadata,
                deltaMetadata,
                hudiMetadata,
                tableProperties,
                materializedViewProperties,
                flushMetadataCache,
                migrateHiveToIcebergProcedure,
                hiveRecursiveDirWalkerEnabled,
                tableTypeCache);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(
                NOT_NULL_COLUMN_CONSTRAINT,
                MATERIALIZED_VIEW_GRACE_PERIOD);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return materializedViewProperties.getProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return analyzeProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return tableProcedures;
    }
}
