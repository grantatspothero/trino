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

import com.google.inject.Inject;
import io.trino.plugin.deltalake.functions.tablechanges.TableChangesTableFunctionHandle;
import io.trino.plugin.hive.functions.Unload.UnloadFunctionHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static java.util.Objects.requireNonNull;

public class ObjectStoreFunctionProvider
        implements FunctionProvider
{
    private final Connector hiveConnector;
    private final Connector deltaConnector;
    private final ObjectStoreSessionProperties objectStoreSessionProperties;

    @Inject
    public ObjectStoreFunctionProvider(@ForHive Connector hiveConnector, @ForDelta Connector deltaConnector, ObjectStoreSessionProperties objectStoreSessionProperties)
    {
        this.hiveConnector = requireNonNull(hiveConnector, "hiveConnector is null");
        this.deltaConnector = requireNonNull(deltaConnector, "deltaConnector is null");
        this.objectStoreSessionProperties = requireNonNull(objectStoreSessionProperties, "objectStoreSessionProperties is null");
    }

    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof UnloadFunctionHandle) {
            return new UnloadProcessorProvider(hiveConnector, objectStoreSessionProperties, functionHandle);
        }
        if (functionHandle instanceof TableChangesTableFunctionHandle) {
            return new DeltaLakeTableChangesProcessorProvider(deltaConnector, objectStoreSessionProperties, functionHandle);
        }
        throw new UnsupportedOperationException("Unsupported function: " + functionHandle);
    }

    private static class UnloadProcessorProvider
            implements TableFunctionProcessorProvider
    {
        private final TableFunctionProcessorProvider tableFunctionProcessorProvider;
        private final ObjectStoreSessionProperties objectStoreSessionProperties;

        public UnloadProcessorProvider(
                Connector hiveConnector,
                ObjectStoreSessionProperties objectStoreSessionProperties,
                ConnectorTableFunctionHandle functionHandle)
        {
            this.objectStoreSessionProperties = requireNonNull(objectStoreSessionProperties, "objectStoreSessionProperties is null");
            FunctionProvider functionProvider = hiveConnector.getFunctionProvider().orElseThrow();
            this.tableFunctionProcessorProvider = requireNonNull(functionProvider.getTableFunctionProcessorProvider(functionHandle), "tableFunctionProcessorProvider is null");
        }

        @Override
        public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle)
        {
            return tableFunctionProcessorProvider.getDataProcessor(objectStoreSessionProperties.unwrap(HIVE, session), handle);
        }
    }

    private static class DeltaLakeTableChangesProcessorProvider
            implements TableFunctionProcessorProvider
    {
        private final TableFunctionProcessorProvider tableFunctionProcessorProvider;
        private final ObjectStoreSessionProperties objectStoreSessionProperties;

        public DeltaLakeTableChangesProcessorProvider(
                Connector deltaConnector,
                ObjectStoreSessionProperties objectStoreSessionProperties,
                ConnectorTableFunctionHandle functionHandle)
        {
            this.objectStoreSessionProperties = requireNonNull(objectStoreSessionProperties, "objectStoreSessionProperties is null");
            FunctionProvider functionProvider = deltaConnector.getFunctionProvider().get();
            this.tableFunctionProcessorProvider = requireNonNull(
                    functionProvider.getTableFunctionProcessorProvider(functionHandle), "tableFunctionProcessorProvider is null");
        }

        @Override
        public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split)
        {
            return tableFunctionProcessorProvider.getSplitProcessor(objectStoreSessionProperties.unwrap(DELTA, session), handle, split);
        }
    }
}
