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
package io.trino.plugin.objectstore.procedure;

import com.google.common.collect.ImmutableList;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.TableId;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.TransactionLogUtil;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.objectstore.ForDelta;
import io.trino.plugin.objectstore.ForIceberg;
import io.trino.plugin.objectstore.GalaxySecurityConfig;
import io.trino.plugin.objectstore.TrinoSecurityControl;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.objectstore.GalaxyIdentity.toDispatchSession;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public final class ObjectStoreRegisterTableProcedure
        implements Provider<Procedure>
{
    private static final String HUDI_METADATA_FOLDER_NAME = ".hoodie";

    private static final MethodHandle REGISTER_TABLE;

    private static final String PROCEDURE_NAME = "register_table";
    private static final String SYSTEM_SCHEMA = "system";

    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String TABLE_LOCATION = "TABLE_LOCATION";
    private static final String METADATA_FILE_NAME = "METADATA_FILE_NAME";

    static {
        try {
            REGISTER_TABLE = lookup().unreflect(ObjectStoreRegisterTableProcedure.class.getMethod("registerTable", ConnectorSession.class, String.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private final ClassLoader classLoader;
    private final LocationAccessControl locationAccessControl;
    private final TrinoSecurityControl securityControl;
    private final CatalogId catalogId;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final Procedure icebergRegisterTable;
    private final Procedure deltaRegisterTable;

    @Inject
    public ObjectStoreRegisterTableProcedure(
            TrinoFileSystemFactory fileSystemFactory,
            LocationAccessControl locationAccessControl,
            TrinoSecurityControl securityControl,
            GalaxySecurityConfig securityConfig,
            @ForIceberg Connector icebergConnector,
            @ForDelta Connector deltaConnector)
    {
        this.classLoader = getClass().getClassLoader();
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.locationAccessControl = requireNonNull(locationAccessControl, "locationAccessControl is null");
        this.securityControl = requireNonNull(securityControl, "securityControl is null");
        this.catalogId = securityConfig.getCatalogId();
        this.icebergRegisterTable = icebergConnector.getProcedures().stream()
                .filter(procedure -> procedure.getName().equals("register_table"))
                .collect(onlyElement());
        this.deltaRegisterTable = deltaConnector.getProcedures().stream()
                .filter(procedure -> procedure.getName().equals("register_table"))
                .collect(onlyElement());
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                SYSTEM_SCHEMA,
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Argument(SCHEMA_NAME, VARCHAR),
                        new Argument(TABLE_NAME, VARCHAR),
                        new Argument(TABLE_LOCATION, VARCHAR),
                        new Argument(METADATA_FILE_NAME, VARCHAR, false, null)),
                REGISTER_TABLE.bindTo(this));
    }

    public void registerTable(
            ConnectorSession session,
            String schemaName,
            String tableName,
            String tableLocation,
            String metadataFileName)
            throws Throwable
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            locationAccessControl.checkCanUseLocation(session.getIdentity(), tableLocation);

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);

            boolean isIcebergTable = exists(fileSystem, format("%s/%s", stripTrailingSlash(tableLocation), IcebergUtil.METADATA_FOLDER_NAME));
            boolean isDeltaLakeTable = exists(fileSystem, format("%s/%s", stripTrailingSlash(tableLocation), TransactionLogUtil.TRANSACTION_LOG_DIRECTORY));
            boolean isHudiTable = exists(fileSystem, format("%s/%s", stripTrailingSlash(tableLocation), HUDI_METADATA_FOLDER_NAME));

            long possibleTableTypesCount = Stream.of(isIcebergTable, isDeltaLakeTable, isHudiTable).filter(bool -> bool).count();
            if (possibleTableTypesCount > 1) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot determine any one of Iceberg, Delta Lake, Hudi table types");
            }

            if (isIcebergTable) {
                icebergRegisterTable.getMethodHandle().invoke(session, schemaName, tableName, tableLocation, metadataFileName);
                // TODO https://github.com/starburstdata/team-lakehouse/issues/213 Move the following TrinoSecurityApi.entityCreated to Iceberg connector considering Tabular integration
                securityControl.entityCreated(toDispatchSession(session.getIdentity()), new TableId(catalogId, schemaName, tableName));
            }
            else if (isDeltaLakeTable) {
                checkProcedureArgument(metadataFileName == null, "Unsupported metadata_file_name argument for Delta Lake table");
                deltaRegisterTable.getMethodHandle().invoke(session, schemaName, tableName, tableLocation);
                securityControl.entityCreated(toDispatchSession(session.getIdentity()), new TableId(catalogId, schemaName, tableName));
            }
            else if (isHudiTable) {
                throw new TrinoException(NOT_SUPPORTED, "Registering Hudi tables is unsupported");
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, "Unsupported table type");
            }
        }
    }

    private static boolean exists(TrinoFileSystem fileSystem, String location)
    {
        try {
            return fileSystem.newInputFile(Location.of(location)).exists();
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to check location: " + location, e);
        }
    }

    private static String stripTrailingSlash(String path)
    {
        checkArgument(path != null && path.length() > 0, "path must not be null or empty");
        return path.replaceFirst("/+$", "");
    }
}
