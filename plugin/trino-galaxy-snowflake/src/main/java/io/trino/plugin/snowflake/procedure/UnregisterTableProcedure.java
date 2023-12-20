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
package io.trino.plugin.snowflake.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.TableId;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.objectstore.TrinoSecurityControl;
import io.trino.plugin.snowflake.GalaxySnowflakeConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static io.trino.plugin.base.galaxy.GalaxyIdentity.toDispatchSession;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.snowflake.procedure.RegisterTableProcedure.STARBURST_FILE_FORMAT_SUFFIX;
import static io.trino.plugin.snowflake.procedure.RegisterTableProcedure.STARBURST_STAGE_SUFFIX;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class UnregisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle UNREGISTER_TABLE;
    private final ConnectionFactory connectionFactory;
    private final boolean unregisterTableProcedureEnabled;
    private final TrinoSecurityControl securityControl;
    private final CatalogId catalogId;
    private final SnowflakeClient snowflakeClient;
    private final IdentifierMapping identifierMapping;

    @Inject
    public UnregisterTableProcedure(ConnectionFactory connectionFactory, GalaxySnowflakeConfig config, TrinoSecurityControl securityControl, SnowflakeClient snowflakeClient, IdentifierMapping identifierMapping)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.unregisterTableProcedureEnabled = requireNonNull(config, "config is null").isExternalTableProceduresEnabled();
        this.securityControl = requireNonNull(securityControl, "securityControl is null");
        this.catalogId = requireNonNull(config, "securityConfig is null").getCatalogId();
        this.snowflakeClient = requireNonNull(snowflakeClient, "snowflakeClient is null");
        this.identifierMapping = requireNonNull(identifierMapping, "identifierMapping is null");
    }

    static {
        try {
            UNREGISTER_TABLE = lookup().unreflect(UnregisterTableProcedure.class.getMethod("unregisterTable", ConnectorSession.class, ConnectorAccessControl.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "unregister_table",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR)),
                UNREGISTER_TABLE.bindTo(this));
    }

    public void unregisterTable(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String schema,
            String table)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doUnregisterTable(session, accessControl, schema, table);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failure when unregistering table %s.%s: %s", schema, table, e), e);
        }
    }

    private void doUnregisterTable(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName)
    {
        if (!unregisterTableProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "unregister_table procedure is disabled");
        }
        checkProcedureArgument(schemaName != null && !schemaName.isEmpty(), "schema_name cannot be null or empty");
        checkProcedureArgument(tableName != null && !tableName.isEmpty(), "table_name cannot be null or empty");

        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        accessControl.checkCanDropTable(null, schemaTableName);

        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement()) {
            RemoteIdentifiers remoteIdentifiers = snowflakeClient.getRemoteIdentifiers(connection);
            String remoteSchema = identifierMapping.toRemoteSchemaName(remoteIdentifiers, session.getIdentity(), schemaName);
            String remoteTable = identifierMapping.toRemoteTableName(remoteIdentifiers, session.getIdentity(), remoteSchema, tableName);

            String stageName = remoteTable + STARBURST_STAGE_SUFFIX;
            String fileFormatName = remoteTable + STARBURST_FILE_FORMAT_SUFFIX;
            String quotedSchemaName = snowflakeClient.quoted(remoteSchema);

            statement.execute(format("DROP EXTERNAL TABLE IF EXISTS %s.%s", quotedSchemaName, snowflakeClient.quoted(remoteTable)));
            statement.execute(format("DROP FILE FORMAT IF EXISTS %s.%s", quotedSchemaName, snowflakeClient.quoted(fileFormatName)));
            statement.execute(format("DROP STAGE IF EXISTS %s.%s", quotedSchemaName, snowflakeClient.quoted(stageName)));

            securityControl.entityDropped(toDispatchSession(session.getIdentity()), new TableId(catalogId, schemaName, tableName));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }
}
