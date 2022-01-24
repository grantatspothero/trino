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
import io.trino.plugin.deltalake.DeltaLakePartitioningHandle;
import io.trino.plugin.deltalake.DeltaLakeUpdateHandle;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.iceberg.IcebergPartitioningHandle;
import io.trino.plugin.iceberg.IcebergUpdateHandle;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;

public class ObjectStoreNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final ConnectorNodePartitioningProvider hiveNodePartitioningProvider;
    private final ConnectorNodePartitioningProvider icebergNodePartitioningProvider;
    private final ConnectorNodePartitioningProvider deltaNodePartitioningProvider;

    @Inject
    public ObjectStoreNodePartitioningProvider(
            @ForHive ConnectorNodePartitioningProvider hiveNodePartitioningProvider,
            @ForIceberg ConnectorNodePartitioningProvider icebergNodePartitioningProvider,
            @ForDelta ConnectorNodePartitioningProvider deltaNodePartitioningProvider)
    {
        this.hiveNodePartitioningProvider = requireNonNull(hiveNodePartitioningProvider, "hiveNodePartitioningProvider is null");
        this.icebergNodePartitioningProvider = requireNonNull(icebergNodePartitioningProvider, "icebergNodePartitioningProvider is null");
        this.deltaNodePartitioningProvider = requireNonNull(deltaNodePartitioningProvider, "deltaNodePartitioningProvider is null");
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return delegate(partitioningHandle, transactionHandle, (partitioningProvider, transaction) ->
                partitioningProvider.getBucketNodeMapping(transactionHandle, session, partitioningHandle));
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        return delegate(partitioningHandle, transactionHandle, (partitioningProvider, transaction) ->
                partitioningProvider.getSplitBucketFunction(transactionHandle, session, partitioningHandle));
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        return delegate(partitioningHandle, transactionHandle, (partitioningProvider, transaction) ->
                partitioningProvider.getBucketFunction(transactionHandle, session, partitioningHandle, partitionChannelTypes, bucketCount));
    }

    private <T> T delegate(ConnectorPartitioningHandle partitioningHandle, ConnectorTransactionHandle transactionHandle, BiFunction<ConnectorNodePartitioningProvider, ConnectorTransactionHandle, T> consumer)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        if (partitioningHandle instanceof HivePartitioningHandle) {
            return consumer.apply(hiveNodePartitioningProvider, transaction.getHiveHandle());
        }
        if ((partitioningHandle instanceof IcebergPartitioningHandle) || (partitioningHandle instanceof IcebergUpdateHandle)) {
            return consumer.apply(icebergNodePartitioningProvider, transaction.getIcebergHandle());
        }
        if ((partitioningHandle instanceof DeltaLakePartitioningHandle) || (partitioningHandle instanceof DeltaLakeUpdateHandle)) {
            return consumer.apply(deltaNodePartitioningProvider, transaction.getDeltaHandle());
        }
        throw new VerifyException("Unhandled class: " + partitioningHandle.getClass().getName());
    }
}
