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
package io.trino.plugin.objectstore.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.Map;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.base.util.Functions.checkFunctionArgument;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TableChangesFunction
        extends AbstractConnectorTableFunction
{
    private static final String SCHEMA_NAME = "system";
    private static final String NAME = "table_changes";
    public static final String SCHEMA_NAME_ARGUMENT = "SCHEMA_NAME";
    private static final String TABLE_NAME_ARGUMENT = "TABLE_NAME";
    private static final String SINCE_VERSION_ARGUMENT = "SINCE_VERSION";

    private final ConnectorTableFunction deltaTableChanges;

    public TableChangesFunction(Connector deltaConnector)
    {
        super(
                SCHEMA_NAME,
                NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder().name(SCHEMA_NAME_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(TABLE_NAME_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(SINCE_VERSION_ARGUMENT).type(BIGINT).defaultValue(null).build()),
                GENERIC_TABLE);
        this.deltaTableChanges = deltaConnector
                .getTableFunctions().stream()
                .filter(connectorTableFunction -> connectorTableFunction.getName().equals("table_changes"))
                .collect(onlyElement());
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
    {
        ScalarArgument schemaNameArgument = (ScalarArgument) arguments.get(SCHEMA_NAME_ARGUMENT);
        checkFunctionArgument(schemaNameArgument.getValue() != null, "schema_name cannot be null");

        ScalarArgument tableNameArgument = (ScalarArgument) arguments.get(TABLE_NAME_ARGUMENT);
        checkFunctionArgument(tableNameArgument.getValue() != null, "table_name value for function table_changes() cannot be null");

        return deltaTableChanges.analyze(session, transaction, arguments, accessControl);
    }
}
