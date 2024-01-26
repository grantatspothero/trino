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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.id.RoleId;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.plugin.warp2.TpchWarpSpeedObjectStoreHudiTablesInitializer;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.DockerTestingAccountFactory;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.spi.Plugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

public final class ObjectStoreQueryRunner
{
    public static final String CATALOG = "objectstore";
    public static final String TPCH_SCHEMA = "tpch";

    private ObjectStoreQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String catalogName = CATALOG;
        private String schemaName = TPCH_SCHEMA;
        private TableType tableType;
        private String s3Url;
        private Map<String, String> hiveS3Config;
        private String metastoreType;
        private TestingGalaxyMetastore metastore;
        private TestingLocationSecurityServer locationSecurityServer;
        private Plugin objectStorePlugin = new ObjectStorePlugin();
        private Map<String, String> extraObjectStoreProperties = ImmutableMap.of();
        private Map<String, String> coordinatorProperties = ImmutableMap.of();
        private Map<String, String> extraProperties = ImmutableMap.of();
        private MockConnectorPlugin mockConnectorPlugin;
        private TestingAccountClient accountClient;
        private boolean useLiveCatalogs = true;

        private Builder() {}

        @CanIgnoreReturnValue
        public Builder withCatalogName(String catalogName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withSchemaName(String schemaName)
        {
            this.schemaName = requireNonNull(schemaName, "schemaName is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withTableType(TableType tableType)
        {
            this.tableType = tableType;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withS3Url(String s3Url)
        {
            this.s3Url = s3Url;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withHiveS3Config(Map<String, String> hiveS3Config)
        {
            this.hiveS3Config = hiveS3Config;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withMetastoreType(String metastoreType)
        {
            this.metastoreType = metastoreType;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withMetastore(TestingGalaxyMetastore metastore)
        {
            this.metastoreType = "galaxy";
            this.metastore = metastore;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withLocationSecurityServer(TestingLocationSecurityServer locationSecurityServer)
        {
            this.locationSecurityServer = locationSecurityServer;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withMockConnectorPlugin(MockConnectorPlugin mockConnectorPlugin)
        {
            this.mockConnectorPlugin = mockConnectorPlugin;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withPlugin(Plugin objectStorePlugin)
        {
            this.objectStorePlugin = objectStorePlugin;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withExtraObjectStoreProperties(Map<String, String> properties)
        {
            this.extraObjectStoreProperties = properties;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withCoordinatorProperties(Map<String, String> properties)
        {
            this.coordinatorProperties = properties;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withExtraProperties(Map<String, String> properties)
        {
            this.extraProperties = properties;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withAccountClient(TestingAccountClient accountClient)
        {
            this.accountClient = accountClient;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withUseLiveCatalogs(boolean useLiveCatalogs)
        {
            this.useLiveCatalogs = useLiveCatalogs;
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            try {
                Map<String, String> properties = createObjectStoreProperties(
                        tableType,
                        ImmutableMap.<String, String>builder()
                                .putAll(locationSecurityServer.getClientConfig())
                                .put("galaxy.catalog-id", accountClient.getOrCreateCatalog("objectstore").getCatalogId().toString())
                                .put("galaxy.access-control-url", accountClient.getBaseUri().toString())
                                .buildOrThrow(),
                        requireNonNull(metastoreType, "metastoreType not set"),
                        metastore != null
                                ? metastore.getMetastoreConfig(s3Url)
                                : Map.of(),
                        hiveS3Config,
                        extraObjectStoreProperties);

                GalaxyQueryRunner.Builder builder = GalaxyQueryRunner.builder(catalogName, schemaName);
                builder.setNodeCount(3);
                builder.setUseLiveCatalogs(useLiveCatalogs);
                builder.setCoordinatorProperties(coordinatorProperties);
                builder.setExtraProperties(extraProperties);
                builder.addPlugin(new TpchPlugin());
                builder.addCatalog(TPCH_SCHEMA, TPCH_SCHEMA, true, ImmutableMap.of());
                builder.addPlugin(objectStorePlugin);
                builder.addCatalog(catalogName, getOnlyElement(objectStorePlugin.getConnectorFactories()).getName(), false, properties);
                builder.addPlugin(new IcebergPlugin());
                if (mockConnectorPlugin != null) {
                    builder.addPlugin(mockConnectorPlugin);
                    builder.addCatalog("mock_dynamic_listing", "mock", false, Map.of());
                }
                builder.setAccountClient(accountClient);
                DistributedQueryRunner queryRunner = builder.build();

                queryRunner.execute("CREATE SCHEMA %s.%s".formatted(catalogName, schemaName));
                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e);
                throw e;
            }
        }
    }

    public static void initializeTpchTables(DistributedQueryRunner queryRunner, Iterable<TpchTable<?>> tables)
    {
        copyTpchTables(
                queryRunner,
                TPCH_SCHEMA,
                TINY_SCHEMA_NAME,
                queryRunner.getDefaultSession(),
                tables);
    }

    public static void initializeTpchTablesHudi(DistributedQueryRunner queryRunner, List<TpchTable<?>> tables, Location externalLocation)
            throws Exception
    {
        TpchObjectStoreHudiTablesInitializer loader = new TpchObjectStoreHudiTablesInitializer(COPY_ON_WRITE, tables);
        loader.initializeTables(queryRunner, externalLocation, TPCH_SCHEMA);
        grantPrivileges(queryRunner, tables);
    }

    public static void initializeWarpSpeedTpchTablesHudi(DistributedQueryRunner queryRunner, TrinoFileSystem trinoFileSystem, TestingGalaxyMetastore metastore, List<TpchTable<?>> tables, Location externalLocation)
            throws Exception
    {
        Path basePath = queryRunner.getCoordinator().getBaseDataDir().resolve("data");
        GalaxyHiveMetastore hiveMetastore = new GalaxyHiveMetastore(metastore.getMetastore(), HDFS_FILE_SYSTEM_FACTORY, basePath.toFile().getAbsolutePath(), new GalaxyHiveMetastoreConfig().isBatchMetadataFetch());
        // TpchObjectStoreHudiTablesInitializer wont work here because it cannot cast CoordinatorDispatcherConnector to ObjectStoreConnector.
        // It is not-trivial to expose the injector or any other internal component of the connector (even just for testing). So TpchWarpSpeedObjectStoreHudiTablesInitializer stays with using the old approach of getting a metastore
        TpchWarpSpeedObjectStoreHudiTablesInitializer loader = new TpchWarpSpeedObjectStoreHudiTablesInitializer(hiveMetastore, trinoFileSystem, COPY_ON_WRITE, tables);
        loader.initializeTables(queryRunner, externalLocation, TPCH_SCHEMA);
        grantPrivileges(queryRunner, tables);
    }

    private static void grantPrivileges(DistributedQueryRunner queryRunner, List<TpchTable<?>> tables)
    {
        for (TpchTable<?> table : tables) {
            queryRunner.execute(format("GRANT SELECT ON objectstore.tpch.%s TO ROLE %s WITH GRANT OPTION", table.getTableName(), ACCOUNT_ADMIN));
            queryRunner.execute(format("GRANT UPDATE ON objectstore.tpch.%s TO ROLE %s WITH GRANT OPTION", table.getTableName(), ACCOUNT_ADMIN));
            queryRunner.execute(format("GRANT DELETE ON objectstore.tpch.%s TO ROLE %s WITH GRANT OPTION", table.getTableName(), ACCOUNT_ADMIN));
            queryRunner.execute(format("GRANT INSERT ON objectstore.tpch.%s TO ROLE %s WITH GRANT OPTION", table.getTableName(), ACCOUNT_ADMIN));
        }
    }

    public static final class ObjectStoreHiveQueryRunner
    {
        private ObjectStoreHiveQueryRunner() {}

        public static void main(String[] args)
                throws Exception
        {
            run(TableType.HIVE);
        }
    }

    public static final class ObjectStoreIcebergQueryRunner
    {
        private ObjectStoreIcebergQueryRunner() {}

        public static void main(String[] args)
                throws Exception
        {
            run(TableType.ICEBERG);
        }
    }

    public static final class ObjectStoreDeltaLakeQueryRunner
    {
        private ObjectStoreDeltaLakeQueryRunner() {}

        public static void main(String[] args)
                throws Exception
        {
            run(TableType.DELTA);
        }
    }

    public static final class ObjectStoreHudiQueryRunner
    {
        private ObjectStoreHudiQueryRunner() {}

        public static void main(String[] args)
                throws Exception
        {
            run(TableType.HUDI);
        }
    }

    static QueryRunner run(TableType tableType)
            throws Exception
    {
        Logging.initialize();

        String bucketName = "test-bucket";
        MinioStorage minio = new MinioStorage(bucketName);
        GalaxyCockroachContainer cockroach = new GalaxyCockroachContainer();
        TestingGalaxyMetastore metastore = new TestingGalaxyMetastore(cockroach);
        TestingLocationSecurityServer locationSecurityServer = new TestingLocationSecurityServer((session, location) -> false);
        minio.start();
        TestingAccountFactory testingAccountFactory = new DockerTestingAccountFactory(cockroach);
        TestingAccountClient account = testingAccountFactory.createAccountClient();

        DistributedQueryRunner queryRunner = builder()
                .withCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .withTableType(tableType)
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getNativeS3Config())
                .withMetastore(metastore)
                .withLocationSecurityServer(locationSecurityServer)
                .withMockConnectorPlugin(new MockConnectorPlugin(MockConnectorFactory.create()))
                .withAccountClient(account)
                .withUseLiveCatalogs(false)
                .build();

        switch (tableType) {
            case HIVE, ICEBERG, DELTA -> initializeTpchTables(queryRunner, TpchTable.getTables());
            case HUDI -> initializeTpchTablesHudi(queryRunner, TpchTable.getTables(), Location.of("s3://%s/hudi/data".formatted(bucketName)));
        }

        Logger log = Logger.get(ObjectStoreQueryRunner.class);
        log.info(
                "\n\n======== SERVER STARTED ========\n" +
                        "Admin:\n" +
                        "  %s\n" +
                        "Public:\n" +
                        "  %s\n" +
                        "====",
                createCliCommand(queryRunner, account, account.getAdminRoleId()),
                createCliCommand(queryRunner, account, account.getPublicRoleId()));
        return queryRunner;
    }

    private static String createCliCommand(DistributedQueryRunner queryRunner, TestingAccountClient account, RoleId roleId)
    {
        return format(
                "client/trino-cli/target/trino-cli-*-executable.jar" +
                        " --server %s" +
                        " --user=%s" +
                        " --catalog=objectstore" +
                        " --schema=tpch" +
                        " --extra-credential=accountId=%s" +
                        " --extra-credential=userId=%s" +
                        " --extra-credential=GalaxyTokenCredential=%s" +
                        " --extra-credential=roleId=%s",
                queryRunner.getCoordinator().getBaseUrl(),
                queryRunner.getDefaultSession().getUser(),
                account.getAccountId(),
                account.getAdminUserId(),
                account.getAdminTrinoAccessToken(),
                roleId);
    }
}
