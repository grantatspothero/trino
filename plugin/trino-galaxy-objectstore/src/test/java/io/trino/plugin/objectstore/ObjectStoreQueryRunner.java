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
        private Plugin objectStorePlugin;
        private String connectorName;
        private Map<String, String> extraObjectStoreProperties;
        private Map<String, String> coordinatorProperties = ImmutableMap.of();
        private MockConnectorPlugin mockConnectorPlugin;
        private GalaxyCockroachContainer cockroach;

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
        public Builder withConnectorName(String connectorName)
        {
            this.connectorName = connectorName;
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

                DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder("objectstore", "tpch")
                        .setNodeCount(3)
                        .setCockroach(cockroach)
                        .setCoordinatorProperties(coordinatorProperties)
                        .addPlugin(new TpchPlugin())
                        .addCatalog(TPCH_SCHEMA, TPCH_SCHEMA, ImmutableMap.of())
                        .addPlugin(objectStorePlugin)
                        .addCatalog(CATALOG, connectorName, properties)
                        .addPlugin(new IcebergPlugin())
                        .addPlugin(this.mockConnectorPlugin)
                        .addCatalog("mock_dynamic_listing", "mock", Map.of())
                        .build();

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

    private static DistributedQueryRunner buildDefaultQueryRunner(TableType tableType, MinioStorage minio, TestingGalaxyMetastore metastore, TestingLocationSecurityServer locationSecurityServer, GalaxyCockroachContainer cockroach)
            throws Exception
    {
        return builder()
                .setCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .withTableType(tableType)
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getHiveS3Config())
                .withMetastore(metastore)
                .withLocationSecurityServer(locationSecurityServer)
                .withCockroach(cockroach)
                .withMockConnectorPlugin(new MockConnectorPlugin(MockConnectorFactory.create()))
                .withPlugin(new ObjectStorePlugin())
                .withConnectorName("galaxy_objectstore")
                .withExtraObjectStoreProperties(ImmutableMap.of())
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
        public static void main(String[] args)
                throws Exception
        {
            loadNecessaryProperties();

            MinioStorage minio = new MinioStorage("test-bucket");
            GalaxyCockroachContainer cockroach = new GalaxyCockroachContainer();
            TestingGalaxyMetastore metastore = new TestingGalaxyMetastore(Optional.of(cockroach));
            TestingLocationSecurityServer locationSecurityServer = new TestingLocationSecurityServer((session, location) -> false);
            minio.start();
            DistributedQueryRunner queryRunner = buildDefaultQueryRunner(TableType.ICEBERG, minio, metastore, locationSecurityServer, cockroach);
            initializeTpchTables(queryRunner, TpchTable.getTables());

            Logger log = Logger.get(ObjectStoreQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class ObjectStoreHiveQueryRunner
    {
        public static void main(String[] args)
                throws Exception
        {
            loadNecessaryProperties();

            MinioStorage minio = new MinioStorage("test-bucket");
            GalaxyCockroachContainer cockroach = new GalaxyCockroachContainer();
            TestingGalaxyMetastore metastore = new TestingGalaxyMetastore(Optional.of(cockroach));
            TestingLocationSecurityServer locationSecurityServer = new TestingLocationSecurityServer((session, location) -> false);
            minio.start();
            DistributedQueryRunner queryRunner = buildDefaultQueryRunner(TableType.HIVE, minio, metastore, locationSecurityServer, cockroach);
            initializeTpchTables(queryRunner, TpchTable.getTables());

            Logger log = Logger.get(ObjectStoreQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class ObjectStoreDeltaLakeQueryRunner
    {
        public static void main(String[] args)
                throws Exception
        {
            loadNecessaryProperties();

            MinioStorage minio = new MinioStorage("test-bucket");
            GalaxyCockroachContainer cockroach = new GalaxyCockroachContainer();
            TestingGalaxyMetastore metastore = new TestingGalaxyMetastore(Optional.of(cockroach));
            TestingLocationSecurityServer locationSecurityServer = new TestingLocationSecurityServer((session, location) -> false);
            minio.start();
            DistributedQueryRunner queryRunner = buildDefaultQueryRunner(TableType.DELTA, minio, metastore, locationSecurityServer, cockroach);
            initializeTpchTables(queryRunner, TpchTable.getTables());

            Logger log = Logger.get(ObjectStoreQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class ObjectStoreHudiQueryRunner
    {
        public static void main(String[] args)
                throws Exception
        {
            loadNecessaryProperties();

            MinioStorage minio = new MinioStorage("test-bucket");
            GalaxyCockroachContainer cockroach = new GalaxyCockroachContainer();
            TestingGalaxyMetastore metastore = new TestingGalaxyMetastore(Optional.of(cockroach));
            TestingLocationSecurityServer locationSecurityServer = new TestingLocationSecurityServer((session, location) -> false);
            minio.start();
            DistributedQueryRunner queryRunner = buildDefaultQueryRunner(TableType.HUDI, minio, metastore, locationSecurityServer, cockroach);

            initializeTpchTablesHudi(queryRunner, TpchTable.getTables(), metastore);

            Logger log = Logger.get(ObjectStoreQueryRunner.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
