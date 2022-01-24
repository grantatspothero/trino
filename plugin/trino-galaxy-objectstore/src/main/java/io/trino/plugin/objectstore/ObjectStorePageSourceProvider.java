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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ObjectStorePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ConnectorPageSourceProvider hivePageSourceProvider;
    private final ConnectorPageSourceProvider icebergPageSourceProvider;
    private final ConnectorPageSourceProvider deltaPageSourceProvider;
    private final ConnectorPageSourceProvider hudiPageSourceProvider;

    @Inject
    public ObjectStorePageSourceProvider(
            @ForHive ConnectorPageSourceProvider hivePageSourceProvider,
            @ForIceberg ConnectorPageSourceProvider icebergPageSourceProvider,
            @ForDelta ConnectorPageSourceProvider deltaPageSourceProvider,
            @ForHudi ConnectorPageSourceProvider hudiPageSourceProvider)
    {
        this.hivePageSourceProvider = requireNonNull(hivePageSourceProvider, "hivePageSourceProvider is null");
        this.icebergPageSourceProvider = requireNonNull(icebergPageSourceProvider, "icebergPageSourceProvider is null");
        this.deltaPageSourceProvider = requireNonNull(deltaPageSourceProvider, "deltaPageSourceProvider is null");
        this.hudiPageSourceProvider = requireNonNull(hudiPageSourceProvider, "hudiPageSourceProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        ObjectStoreTransactionHandle transaction = (ObjectStoreTransactionHandle) transactionHandle;
        if (table instanceof HiveTableHandle) {
            return hivePageSourceProvider.createPageSource(transaction.getHiveHandle(), session, split, table, columns, dynamicFilter);
        }
        if (table instanceof IcebergTableHandle) {
            return icebergPageSourceProvider.createPageSource(transaction.getIcebergHandle(), session, split, table, columns, dynamicFilter);
        }
        if (table instanceof DeltaLakeTableHandle) {
            return deltaPageSourceProvider.createPageSource(transaction.getDeltaHandle(), session, split, table, columns, dynamicFilter);
        }
        if (table instanceof HudiTableHandle) {
            return hudiPageSourceProvider.createPageSource(transaction.getHudiHandle(), session, split, table, columns, dynamicFilter);
        }
        throw new VerifyException("Unhandled class: " + table.getClass().getName());
    }
}
