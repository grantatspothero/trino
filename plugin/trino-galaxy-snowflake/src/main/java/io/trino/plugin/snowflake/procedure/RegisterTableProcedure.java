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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeClient;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.TableId;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.hive.LocationAccessControl;
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
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class RegisterTableProcedure
        implements Provider<Procedure>
{
    public static final String STARBURST_STAGE_SUFFIX = "_STARBURST_STAGE";
    public static final String STARBURST_FILE_FORMAT_SUFFIX = "_STARBURST_FILE_FORMAT";

    private static final MethodHandle REGISTER_TABLE;
    private final SnowflakeClient snowflakeClient;
    private final ConnectionFactory connectionFactory;
    private final boolean registerTableProcedureEnabled;
    private final TrinoSecurityControl securityControl;
    private final CatalogId catalogId;
    private final LocationAccessControl locationAccessControl;
    private final IdentifierMapping identifierMapping;

    @Inject
    public RegisterTableProcedure(SnowflakeClient snowflakeClient, ConnectionFactory connectionFactory, GalaxySnowflakeConfig config, TrinoSecurityControl securityControl, LocationAccessControl locationAccessControl, IdentifierMapping identifierMapping)
    {
        this.snowflakeClient = requireNonNull(snowflakeClient, "client is null");
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.registerTableProcedureEnabled = requireNonNull(config, "config is null").isExternalTableProceduresEnabled();
        this.securityControl = requireNonNull(securityControl, "securityControl is null");
        this.locationAccessControl = requireNonNull(locationAccessControl, "locationAccessControl is null");
        this.catalogId = config.getCatalogId();
        this.identifierMapping = requireNonNull(identifierMapping, "identifierMapping is null");
    }

    static {
        try {
            REGISTER_TABLE = lookup().unreflect(RegisterTableProcedure.class.getMethod("registerTable", ConnectorSession.class, ConnectorAccessControl.class, String.class, String.class, String.class, String.class));
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
                "register_table",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_LOCATION", VARCHAR),
                        new Procedure.Argument("STORAGE_INTEGRATION", VARCHAR)),
                REGISTER_TABLE.bindTo(this));
    }

    public void registerTable(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String schema,
            String table,
            String tableLocation,
            String storageIntegration)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterTable(session, accessControl, schema, table, tableLocation, storageIntegration);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failure when registering table %s.%s at location '%s': %s", schema, table, tableLocation, e), e);
        }
    }

    private void doRegisterTable(ConnectorSession session, ConnectorAccessControl accessControl, String schemaName, String tableName, String tableLocation, String storageIntegration)
    {
        if (!registerTableProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "register_table procedure is disabled");
        }
        locationAccessControl.checkCanUseLocation(session.getIdentity(), tableLocation);

        checkProcedureArgument(schemaName != null && !schemaName.isEmpty(), "schema_name cannot be null or empty");
        checkProcedureArgument(tableName != null && !tableName.isEmpty(), "table_name cannot be null or empty");
        checkProcedureArgument(tableLocation != null && !tableLocation.isEmpty(), "table_location cannot be null or empty");
        checkProcedureArgument(storageIntegration != null && !storageIntegration.isEmpty(), "storage_integration cannot be null or empty");

        if (!snowflakeClient.schemaExists(session, schemaName)) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName));
        }
        // TODO: validate the location has only iceberg tables. This will need the connector to be extended to take s3 credentials
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        accessControl.checkCanCreateTable(null, schemaTableName, ImmutableMap.of());

        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement()) {
            RemoteIdentifiers remoteIdentifiers = snowflakeClient.getRemoteIdentifiers(connection);
            String remoteSchema = identifierMapping.toRemoteSchemaName(remoteIdentifiers, session.getIdentity(), schemaTableName.getSchemaName());
            String remoteTable = identifierMapping.toRemoteTableName(remoteIdentifiers, session.getIdentity(), remoteSchema, schemaTableName.getTableName());

            String stageName = remoteTable + STARBURST_STAGE_SUFFIX;
            String fileFormatName = remoteTable + STARBURST_FILE_FORMAT_SUFFIX;
            String quotedSchemaName = snowflakeClient.quoted(remoteSchema);

            String qualifiedTableName = quotedSchemaName + "." + snowflakeClient.quoted(remoteTable);
            String qualifiedStageName = quotedSchemaName + "." + snowflakeClient.quoted(stageName);
            String qualifiedFileFormatName = quotedSchemaName + "." + snowflakeClient.quoted(fileFormatName);

            String createStageSql = format("CREATE STAGE %s URL = '%s' STORAGE_INTEGRATION = %s", qualifiedStageName, tableLocation, storageIntegration);
            String fileFormatSql = format("CREATE FILE FORMAT %s TYPE = PARQUET", qualifiedFileFormatName);

            String createTableSql = format("""
                                            CREATE EXTERNAL TABLE %s
                                                USING TEMPLATE(
                                                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(
                                                        INFER_SCHEMA(LOCATION=>'@%s', FILE_FORMAT=> '%s')))
                                                LOCATION=@%s
                                                FILE_FORMAT=%s
                    """, qualifiedTableName, qualifiedStageName, qualifiedFileFormatName, qualifiedStageName, qualifiedFileFormatName);
            statement.execute(createStageSql);
            statement.execute(fileFormatSql);
            statement.execute(createTableSql);

            securityControl.entityCreated(toDispatchSession(session.getIdentity()), new TableId(catalogId, schemaName, tableName));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }
}
