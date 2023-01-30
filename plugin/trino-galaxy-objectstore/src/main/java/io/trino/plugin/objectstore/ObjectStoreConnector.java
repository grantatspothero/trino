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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Sets.immutableEnumSet;
import static com.google.common.collect.Sets.symmetricDifference;
import static com.google.common.collect.Streams.forEachPair;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.verifyPropertyDescription;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.verifyPropertyMetadata;
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
    private final ObjectStoreSplitManager splitManager;
    private final ObjectStorePageSourceProvider pageSourceProvider;
    private final ObjectStorePageSinkProvider pageSinkProvider;
    private final ObjectStoreNodePartitioningProvider nodePartitioningProvider;
    private final ObjectStoreTableProperties tableProperties;
    private final ObjectStoreMaterializedViewProperties materializedViewProperties;
    private final Set<Procedure> procedures;

    @Inject
    public ObjectStoreConnector(
            @ForHive Connector hiveConnector,
            @ForIceberg Connector icebergConnector,
            @ForDelta Connector deltaConnector,
            @ForHudi Connector hudiConnector,
            ObjectStoreSplitManager splitManager,
            ObjectStorePageSourceProvider pageSourceProvider,
            ObjectStorePageSinkProvider pageSinkProvider,
            ObjectStoreNodePartitioningProvider nodePartitioningProvider,
            ObjectStoreTableProperties tableProperties,
            ObjectStoreMaterializedViewProperties materializedViewProperties,
            Set<Procedure> procedures)
    {
        this.hiveConnector = requireNonNull(hiveConnector, "hiveConnector is null");
        this.icebergConnector = requireNonNull(icebergConnector, "icebergConnector is null");
        this.deltaConnector = requireNonNull(deltaConnector, "deltaConnector is null");
        this.hudiConnector = requireNonNull(hudiConnector, "hudiConnector is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.materializedViewProperties = requireNonNull(materializedViewProperties, "materializedViewProperties is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
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

        return new ObjectStoreMetadata(hiveMetadata, icebergMetadata, deltaMetadata, hudiMetadata, tableProperties, materializedViewProperties);
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
        return immutableEnumSet(NOT_NULL_COLUMN_CONSTRAINT);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        Set<String> ignoredDescriptions = ImmutableSet.<String>builder()
                .add("compression_codec")
                .add("projection_pushdown_enabled")
                .add("timestamp_precision")
                .add("minimum_assigned_split_weight")
                .build();

        Map<String, PropertyMetadata<?>> properties = new HashMap<>();
        for (Connector connector : ImmutableSet.of(hiveConnector, icebergConnector, deltaConnector, hudiConnector)) {
            for (PropertyMetadata<?> property : connector.getSessionProperties()) {
                PropertyMetadata<?> existing = properties.putIfAbsent(property.getName(), property);
                if (existing != null) {
                    verifyPropertyMetadata(property, existing);
                    if (!ignoredDescriptions.contains(property.getName())) {
                        verifyPropertyDescription(property, existing);
                    }
                }
            }
        }
        return ImmutableList.copyOf(properties.values());
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        "location",
                        "Base file system location URI",
                        null,
                        false))
                .build();
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
        Map<String, PropertyMetadata<?>> properties = new HashMap<>();
        for (Connector connector : ImmutableSet.of(hiveConnector, icebergConnector, deltaConnector, hudiConnector)) {
            for (PropertyMetadata<?> property : connector.getAnalyzeProperties()) {
                PropertyMetadata<?> existing = properties.putIfAbsent(property.getName(), property);
                if (existing != null) {
                    verifyPropertyMetadata(property, existing);
                    verifyPropertyDescription(property, existing);
                }
            }
        }
        return ImmutableList.copyOf(properties.values());
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        verify(hiveConnector.getColumnProperties().stream().allMatch(
                property -> property.getName().startsWith("partition_projection_")), "Unexpected Hive column properties");
        verify(icebergConnector.getColumnProperties().isEmpty(), "Unexpected Iceberg column properties");
        verify(deltaConnector.getColumnProperties().isEmpty(), "Unexpected Delta Lake column properties");
        verify(hudiConnector.getColumnProperties().isEmpty(), "Unexpected Hudi column properties");
        return ImmutableList.of();
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        Map<String, Procedure> procedures = new HashMap<>();
        this.procedures.forEach(procedure -> procedures.put(procedure.getName(), procedure));
        for (Connector connector : ImmutableSet.of(hiveConnector, icebergConnector, deltaConnector, hudiConnector)) {
            for (Procedure procedure : connector.getProcedures()) {
                String name = procedure.getName();
                if (name.equals("migrate") || name.equals("register_table") || name.equals("unregister_table")) {
                    // Ignore connector-specific procedures if they exist.
                    // This needs to be provided in an ObjectStore-specific manner.
                    continue;
                }
                Procedure existing = procedures.putIfAbsent(name, procedure);
                if (existing == null) {
                    continue;
                }
                if (!name.equals("flush_metadata_cache")) {
                    throw new VerifyException("Duplicate procedure: " + name);
                }
            }
        }
        return ImmutableSet.copyOf(procedures.values());
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        Map<String, TableProcedureMetadata> procedures = new HashMap<>();
        for (Connector connector : ImmutableSet.of(hiveConnector, icebergConnector, deltaConnector, hudiConnector)) {
            for (TableProcedureMetadata procedure : connector.getTableProcedures()) {
                String name = procedure.getName();
                verify(name.equals(name.toUpperCase(Locale.ROOT)), "Procedure name is not uppercase: %s", name);

                TableProcedureMetadata existing = procedures.putIfAbsent(name, procedure);
                if (existing == null) {
                    continue;
                }

                if (!name.equals("OPTIMIZE")) {
                    throw new VerifyException("Duplicate procedure: " + name);
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
                        (property, other) -> {
                            verifyPropertyMetadata(property, other);
                            verifyPropertyDescription(property, other);
                        });
            }
        }
        return ImmutableSet.copyOf(procedures.values());
    }
}
