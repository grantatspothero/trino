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
package io.trino.plugin.warp;

import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeSplit;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.iceberg.CorruptedIcebergTableHandle;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherStatisticsProvider;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.plugin.warp.proxiedconnector.deltalake.DeltaLakeProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.hive.HiveProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.hudi.HudiProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.iceberg.IcebergProxiedConnectorTransformer;
import io.trino.spi.Node;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ObjectStoreProxiedConnectorTransformer
        implements DispatcherProxiedConnectorTransformer
{
    private final DeltaLakeProxiedConnectorTransformer deltaLakeProxiedConnectorTransformer;
    private final HiveProxiedConnectorTransformer hiveProxiedConnectorTransformer;
    private final IcebergProxiedConnectorTransformer icebergProxiedConnectorTransformer;
    private final HudiProxiedConnectorTransformer hudiProxiedConnectorTransformer;

    @Inject
    public ObjectStoreProxiedConnectorTransformer(
            DeltaLakeProxiedConnectorTransformer deltaLakeProxiedConnectorTransformer,
            HiveProxiedConnectorTransformer hiveProxiedConnectorTransformer,
            IcebergProxiedConnectorTransformer icebergProxiedConnectorTransformer,
            HudiProxiedConnectorTransformer hudiProxiedConnectorTransformer)
    {
        this.deltaLakeProxiedConnectorTransformer = requireNonNull(deltaLakeProxiedConnectorTransformer);
        this.hiveProxiedConnectorTransformer = requireNonNull(hiveProxiedConnectorTransformer);
        this.icebergProxiedConnectorTransformer = requireNonNull(icebergProxiedConnectorTransformer);
        this.hudiProxiedConnectorTransformer = requireNonNull(hudiProxiedConnectorTransformer);
    }

    @Override
    public Map<String, Integer> calculateColumnsStatisticsBucketPriority(
            DispatcherStatisticsProvider statisticsProvider,
            Map<ColumnHandle, ColumnStatistics> columnStatistics)
    {
        if (columnStatistics.isEmpty()) {
            return Map.of();
        }
        return getTransformer(columnStatistics.keySet().stream().findFirst().orElseThrow())
                .calculateColumnsStatisticsBucketPriority(statisticsProvider, columnStatistics);
    }

    @Override
    public DispatcherSplit createDispatcherSplit(
            ConnectorSplit proxyConnectorSplit,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor,
            ConnectorSession session)
    {
        return getTransformer(proxyConnectorSplit)
                .createDispatcherSplit(proxyConnectorSplit,
                        dispatcherTableHandle,
                        connectorSplitNodeDistributor,
                        session);
    }

    @Override
    public ConnectorTableHandle createProxyTableHandleForWarming(DispatcherTableHandle dispatcherTableHandle)
    {
        return getTransformer(dispatcherTableHandle.getProxyConnectorTableHandle())
                .createProxyTableHandleForWarming(dispatcherTableHandle);
    }

    @Override
    public RegularColumn getVaradaRegularColumn(ColumnHandle columnHandle)
    {
        return getTransformer(columnHandle).getVaradaRegularColumn(columnHandle);
    }

    @Override
    public Type getColumnType(ColumnHandle columnHandle)
    {
        return getTransformer(columnHandle).getColumnType(columnHandle);
    }

    @Override
    public Object getConvertedPartitionValue(RowGroupData rowGroupData, ColumnHandle columnHandle)
    {
        return getTransformer(columnHandle).getConvertedPartitionValue(rowGroupData, columnHandle);
    }

    @Override
    public boolean isValidForAcceleration(DispatcherTableHandle dispatcherTableHandle)
    {
        return getTransformer(dispatcherTableHandle.getProxyConnectorTableHandle()).isValidForAcceleration(dispatcherTableHandle);
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        return getTransformer(connectorTableHandle).getSchemaTableName(connectorTableHandle);
    }

    @Override
    public ConnectorTableHandle createProxiedConnectorTableHandleForMixedQuery(DispatcherTableHandle dispatcherTableHandle)
    {
        return getTransformer(dispatcherTableHandle.getProxyConnectorTableHandle())
                .createProxiedConnectorTableHandleForMixedQuery(dispatcherTableHandle);
    }

    @Override
    public ConnectorSplit createProxiedConnectorSplitForMixedQuery(ConnectorSplit connectorSplit)
    {
        return getTransformer(connectorSplit).createProxiedConnectorSplitForMixedQuery(connectorSplit);
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, ConnectorNodePartitioningProvider nodePartitionProvider, List<Node> nodes)
    {
        return nodePartitionProvider.getBucketNodeMapping(
                transactionHandle,
                session,
                partitioningHandle);
    }

    private DispatcherProxiedConnectorTransformer getTransformer(ColumnHandle columnHandle)
    {
        if (columnHandle instanceof DeltaLakeColumnHandle) {
            return deltaLakeProxiedConnectorTransformer;
        }
        if (columnHandle instanceof HiveColumnHandle) {
            return hiveProxiedConnectorTransformer;
        }
        if (columnHandle instanceof IcebergColumnHandle) {
            return icebergProxiedConnectorTransformer;
        }
//      no hudi since its using HiveColumnHandle internally
        throw new UnsupportedOperationException("columnHandle not supported for " + columnHandle.getClass());
    }

    private DispatcherProxiedConnectorTransformer getTransformer(ConnectorSplit connectorSplit)
    {
        if (connectorSplit instanceof DeltaLakeSplit) {
            return deltaLakeProxiedConnectorTransformer;
        }
        if (connectorSplit instanceof HiveSplit) {
            return hiveProxiedConnectorTransformer;
        }
        if (connectorSplit instanceof IcebergSplit) {
            return icebergProxiedConnectorTransformer;
        }
        if (connectorSplit instanceof HudiSplit) {
            return hudiProxiedConnectorTransformer;
        }
        throw new UnsupportedOperationException("connectorSplit not supported for " + connectorSplit.getClass());
    }

    private DispatcherProxiedConnectorTransformer getTransformer(ConnectorTableHandle connectorTableHandle)
    {
        if (connectorTableHandle instanceof DeltaLakeTableHandle) {
            return deltaLakeProxiedConnectorTransformer;
        }
        if (connectorTableHandle instanceof HiveTableHandle) {
            return hiveProxiedConnectorTransformer;
        }
        if (connectorTableHandle instanceof IcebergTableHandle || connectorTableHandle instanceof CorruptedIcebergTableHandle) {
            return icebergProxiedConnectorTransformer;
        }
        if (connectorTableHandle instanceof HudiTableHandle) {
            return hudiProxiedConnectorTransformer;
        }
        throw new UnsupportedOperationException("connectorTableHandle not supported for " + connectorTableHandle.getClass());
    }
}
