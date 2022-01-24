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
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ObjectStoreSplitManager
        implements ConnectorSplitManager
{
    private final ConnectorSplitManager hiveSplitManager;
    private final ConnectorSplitManager icebergSplitManager;
    private final ConnectorSplitManager deltaSplitManager;
    private final ConnectorSplitManager hudiSplitManager;

    @Inject
    public ObjectStoreSplitManager(
            @ForHive ConnectorSplitManager hiveSplitManager,
            @ForIceberg ConnectorSplitManager icebergSplitManager,
            @ForDelta ConnectorSplitManager deltaSplitManager,
            @ForHudi ConnectorSplitManager hudiSplitManager)
    {
        this.hiveSplitManager = requireNonNull(hiveSplitManager, "hiveSplitManager is null");
        this.icebergSplitManager = requireNonNull(icebergSplitManager, "icebergSplitManager is null");
        this.deltaSplitManager = requireNonNull(deltaSplitManager, "deltaSplitManager is null");
        this.hudiSplitManager = requireNonNull(hudiSplitManager, "hudiSplitManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        if (table instanceof HiveTableHandle) {
            return hiveSplitManager.getSplits(transaction.getHiveHandle(), session, table, dynamicFilter, constraint);
        }
        if (table instanceof IcebergTableHandle) {
            return icebergSplitManager.getSplits(transaction.getIcebergHandle(), session, table, dynamicFilter, constraint);
        }
        if (table instanceof DeltaLakeTableHandle) {
            return deltaSplitManager.getSplits(transaction.getDeltaHandle(), session, table, dynamicFilter, constraint);
        }
        if (table instanceof HudiTableHandle) {
            return hudiSplitManager.getSplits(transaction.getHudiHandle(), session, table, dynamicFilter, constraint);
        }
        throw new VerifyException("Unhandled class: " + table.getClass().getName());
    }
}
