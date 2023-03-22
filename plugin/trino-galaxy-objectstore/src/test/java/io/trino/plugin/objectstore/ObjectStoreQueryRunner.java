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
import com.google.common.io.Resources;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.DockerTestingAccountFactory;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.spi.Plugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.tpch.TpchTable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.objectstore.GalaxyIdentity.createIdentity;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

public class ObjectStoreQueryRunner
{
    private static final String CATALOG = "objectstore";
    private static final String TPCH_SCHEMA = "tpch";
    private static final String STARGATE_VERSION_PROPERTY_KEY = "stargate.version";

    private ObjectStoreQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private TableType tableType;
        private String s3Url;
        private Map<String, String> hiveS3Config;
        private TestingGalaxyMetastore metastore;
        private TestingLocationSecurityServer locationSecurityServer;
        private Plugin objectStorePlugin = new ObjectStorePlugin();
        private Map<String, String> extraObjectStoreProperties = ImmutableMap.of();
        private Map<String, String> coordinatorProperties = ImmutableMap.of();
        private MockConnectorPlugin mockConnectorPlugin;
        private GalaxyCockroachContainer cockroach;
        private TestingAccountClient accountClient;

        private Builder()
        {
            super(testSessionBuilder()
                    .setIdentity(createIdentity(
                            "user",
                            new AccountId("a-12345678"),
                            new UserId("u-1234567890"),
                            new RoleId("r-1234567890"),
                            "testToken"))
                    .setCatalog(CATALOG)
                    .setSchema(TPCH_SCHEMA)
                    .build());
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
        public Builder withMetastore(TestingGalaxyMetastore metastore)
        {
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
        public Builder withCockroach(GalaxyCockroachContainer cockroach)
        {
            this.cockroach = cockroach;
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
        public Builder withAccountClient(TestingAccountClient accountClient)
        {
            this.accountClient = accountClient;
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            try {
                Map<String, String> properties = createObjectStoreProperties(
                        tableType,
                        locationSecurityServer.getClientConfig(),
                        metastore.getMetastoreConfig(s3Url),
                        hiveS3Config,
                        extraObjectStoreProperties);

                GalaxyQueryRunner.Builder builder = GalaxyQueryRunner.builder("objectstore", "tpch");
                builder.setNodeCount(3);
                builder.setCockroach(cockroach);
                builder.setCoordinatorProperties(coordinatorProperties);
                builder.addPlugin(new TpchPlugin());
                builder.addCatalog(TPCH_SCHEMA, TPCH_SCHEMA, ImmutableMap.of());
                builder.addPlugin(objectStorePlugin);
                builder.addCatalog(CATALOG, getOnlyElement(objectStorePlugin.getConnectorFactories()).getName(), properties);
                builder.addPlugin(new IcebergPlugin());
                if (mockConnectorPlugin != null) {
                    builder.addPlugin(mockConnectorPlugin);
                    builder.addCatalog("mock_dynamic_listing", "mock", Map.of());
                }
                builder.setAccountClient(Optional.ofNullable(accountClient));
                DistributedQueryRunner queryRunner = builder.build();

                queryRunner.execute("CREATE SCHEMA objectstore.tpch");
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

    public static void initializeTpchTablesHudi(DistributedQueryRunner queryRunner, List<TpchTable<?>> tables, TestingGalaxyMetastore metastore)
    {
        String dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("data").toString();
        HiveMetastore hiveMetastore = new GalaxyHiveMetastore(metastore.getMetastore(), HDFS_ENVIRONMENT, dataDir);
        TpchHudiTablesInitializer loader = new TpchHudiTablesInitializer(COPY_ON_WRITE, tables);
        loader.initializeTables(queryRunner, hiveMetastore, TPCH_SCHEMA, dataDir, newEmptyConfiguration());
        for (TpchTable<?> table : tables) {
            queryRunner.execute(format("GRANT SELECT ON objectstore.tpch.%s TO ROLE %s WITH GRANT OPTION", table.getTableName(), ACCOUNT_ADMIN));
            queryRunner.execute(format("GRANT UPDATE ON objectstore.tpch.%s TO ROLE %s WITH GRANT OPTION", table.getTableName(), ACCOUNT_ADMIN));
            queryRunner.execute(format("GRANT DELETE ON objectstore.tpch.%s TO ROLE %s WITH GRANT OPTION", table.getTableName(), ACCOUNT_ADMIN));
            queryRunner.execute(format("GRANT INSERT ON objectstore.tpch.%s TO ROLE %s WITH GRANT OPTION", table.getTableName(), ACCOUNT_ADMIN));
        }
    }

    private static DistributedQueryRunner buildDefaultQueryRunner(TableType tableType, MinioStorage minio, TestingGalaxyMetastore metastore, TestingLocationSecurityServer locationSecurityServer, GalaxyCockroachContainer cockroach, TestingAccountClient account)
            throws Exception
    {
        return builder()
                .withCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .withTableType(tableType)
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getHiveS3Config())
                .withMetastore(metastore)
                .withLocationSecurityServer(locationSecurityServer)
                .withCockroach(cockroach)
                .withMockConnectorPlugin(new MockConnectorPlugin(MockConnectorFactory.create()))
                .withAccountClient(account)
                .build();
    }

    private static void loadNecessaryProperties()
    {
        Properties properties = new Properties();
        try {
            try (InputStream stream = Resources.getResource("objectstore.properties").openStream()) {
                properties.load(stream);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // stargate.version is required by io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore
        System.setProperty(STARGATE_VERSION_PROPERTY_KEY, properties.getProperty(STARGATE_VERSION_PROPERTY_KEY));
    }

    public static final class ObjectStoreIcebergQueryRunner
    {
        private ObjectStoreIcebergQueryRunner() {}

        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            loadNecessaryProperties();

            MinioStorage minio = new MinioStorage("test-bucket");
            GalaxyCockroachContainer cockroach = new GalaxyCockroachContainer();
            TestingGalaxyMetastore metastore = new TestingGalaxyMetastore(Optional.of(cockroach));
            TestingLocationSecurityServer locationSecurityServer = new TestingLocationSecurityServer((session, location) -> false);
            minio.start();
            @SuppressWarnings("resource")
            TestingAccountFactory testingAccountFactory = new DockerTestingAccountFactory(Optional.of(cockroach));
            TestingAccountClient account = testingAccountFactory.createAccount();
            DistributedQueryRunner queryRunner = buildDefaultQueryRunner(TableType.ICEBERG, minio, metastore, locationSecurityServer, cockroach, account);
            initializeTpchTables(queryRunner, TpchTable.getTables());

            Logger log = Logger.get(ObjectStoreIcebergQueryRunner.class);
            log.info(
                    "\n\n======== SERVER STARTED ========\n" +
                            "Admin:\n" +
                            "  %s\n" +
                            "Public:\n" +
                            "  %s\n" +
                            "====",
                    createCliCommand(queryRunner, account, account.getAdminRoleId()),
                    createCliCommand(queryRunner, account, account.getPublicRoleId()));
        }
    }

    public static final class ObjectStoreHiveQueryRunner
    {
        private ObjectStoreHiveQueryRunner() {}

        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            loadNecessaryProperties();

            MinioStorage minio = new MinioStorage("test-bucket");
            GalaxyCockroachContainer cockroach = new GalaxyCockroachContainer();
            TestingGalaxyMetastore metastore = new TestingGalaxyMetastore(Optional.of(cockroach));
            TestingLocationSecurityServer locationSecurityServer = new TestingLocationSecurityServer((session, location) -> false);
            minio.start();
            @SuppressWarnings("resource")
            TestingAccountFactory testingAccountFactory = new DockerTestingAccountFactory(Optional.of(cockroach));
            TestingAccountClient account = testingAccountFactory.createAccount();
            DistributedQueryRunner queryRunner = buildDefaultQueryRunner(TableType.HIVE, minio, metastore, locationSecurityServer, cockroach, account);
            initializeTpchTables(queryRunner, TpchTable.getTables());

            Logger log = Logger.get(ObjectStoreHiveQueryRunner.class);
            log.info(
                    "\n\n======== SERVER STARTED ========\n" +
                            "Admin:\n" +
                            "  %s\n" +
                            "Public:\n" +
                            "  %s\n" +
                            "====",
                    createCliCommand(queryRunner, account, account.getAdminRoleId()),
                    createCliCommand(queryRunner, account, account.getPublicRoleId()));
        }
    }

    public static final class ObjectStoreDeltaLakeQueryRunner
    {
        private ObjectStoreDeltaLakeQueryRunner() {}

        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            loadNecessaryProperties();

            MinioStorage minio = new MinioStorage("test-bucket");
            GalaxyCockroachContainer cockroach = new GalaxyCockroachContainer();
            TestingGalaxyMetastore metastore = new TestingGalaxyMetastore(Optional.of(cockroach));
            TestingLocationSecurityServer locationSecurityServer = new TestingLocationSecurityServer((session, location) -> false);
            minio.start();
            @SuppressWarnings("resource")
            TestingAccountFactory testingAccountFactory = new DockerTestingAccountFactory(Optional.of(cockroach));
            TestingAccountClient account = testingAccountFactory.createAccount();
            DistributedQueryRunner queryRunner = buildDefaultQueryRunner(TableType.DELTA, minio, metastore, locationSecurityServer, cockroach, account);
            initializeTpchTables(queryRunner, TpchTable.getTables());

            Logger log = Logger.get(ObjectStoreDeltaLakeQueryRunner.class);
            log.info(
                    "\n\n======== SERVER STARTED ========\n" +
                            "Admin:\n" +
                            "  %s\n" +
                            "Public:\n" +
                            "  %s\n" +
                            "====",
                    createCliCommand(queryRunner, account, account.getAdminRoleId()),
                    createCliCommand(queryRunner, account, account.getPublicRoleId()));
        }
    }

    public static final class ObjectStoreHudiQueryRunner
    {
        private ObjectStoreHudiQueryRunner() {}

        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();
            loadNecessaryProperties();

            MinioStorage minio = new MinioStorage("test-bucket");
            GalaxyCockroachContainer cockroach = new GalaxyCockroachContainer();
            TestingGalaxyMetastore metastore = new TestingGalaxyMetastore(Optional.of(cockroach));
            TestingLocationSecurityServer locationSecurityServer = new TestingLocationSecurityServer((session, location) -> false);
            minio.start();
            @SuppressWarnings("resource")
            TestingAccountFactory testingAccountFactory = new DockerTestingAccountFactory(Optional.of(cockroach));
            TestingAccountClient account = testingAccountFactory.createAccount();
            DistributedQueryRunner queryRunner = buildDefaultQueryRunner(TableType.HUDI, minio, metastore, locationSecurityServer, cockroach, account);

            initializeTpchTablesHudi(queryRunner, TpchTable.getTables(), metastore);

            Logger log = Logger.get(ObjectStoreHudiQueryRunner.class);
            log.info(
                    "\n\n======== SERVER STARTED ========\n" +
                            "Admin:\n" +
                            "  %s\n" +
                            "Public:\n" +
                            "  %s\n" +
                            "====",
                    createCliCommand(queryRunner, account, account.getAdminRoleId()),
                    createCliCommand(queryRunner, account, account.getPublicRoleId()));
        }
    }

    private static String createCliCommand(DistributedQueryRunner queryRunner, TestingAccountClient account, RoleId roleId)
    {
        return format(
                "client/trino-cli/target/trino-cli-*-executable.jar" +
                        " --server %s" +
                        " --user=%s" +
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
