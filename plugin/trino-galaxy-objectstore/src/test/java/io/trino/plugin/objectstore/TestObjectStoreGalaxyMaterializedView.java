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
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.SchemaId;
import io.starburst.stargate.metastore.client.Table;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.security.galaxy.GalaxyTestHelper;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.starburst.stargate.accesscontrol.privilege.GrantKind.ALLOW;
import static io.trino.plugin.objectstore.MinioStorage.ACCESS_KEY;
import static io.trino.plugin.objectstore.MinioStorage.SECRET_KEY;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test ObjectStore connector materialized views with Galaxy metastore.
 */
public class TestObjectStoreGalaxyMaterializedView
        extends BaseObjectStoreMaterializedViewTest
{
    private static final String TEST_CATALOG = "iceberg";

    private final String bucketName = "test-bucket-" + randomNameSuffix();
    private GalaxyTestHelper galaxyTestHelper;
    private TestingGalaxyMetastore metastore;
    private String schemaDirectory;

    @Override
    protected String getSchemaDirectory()
    {
        return schemaDirectory;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        galaxyTestHelper = closeAfterClass(new GalaxyTestHelper());
        galaxyTestHelper.initialize();

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> false));

        MinioStorage minio = closeAfterClass(new MinioStorage(bucketName));
        minio.start();
        schemaDirectory = minio.getS3Url() + "/" + "default";
        metastore = closeAfterClass(new TestingGalaxyMetastore(galaxyTestHelper.getCockroach()));

        Map<String, String> properties = createObjectStoreProperties(
                ICEBERG,
                ImmutableMap.<String, String>builder()
                        .putAll(locationSecurityServer.getClientConfig())
                        .put("galaxy.catalog-id", "c-1234567890")
                        .buildOrThrow(),
                "galaxy",
                metastore.getMetastoreConfig(minio.getS3Url()),
                minio.getHiveS3Config(),
                Map.of());
        DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder(TEST_CATALOG, "default")
                .setAccountClient(galaxyTestHelper.getAccountClient())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new ObjectStorePlugin())
                .addCatalog(TEST_CATALOG, "galaxy_objectstore", false, properties)
                .addPlugin(new TpchPlugin())
                .addCatalog("tpch", "tpch", true, ImmutableMap.of())
                .build();
        queryRunner.execute("CREATE SCHEMA %s.%s".formatted(TEST_CATALOG, queryRunner.getDefaultSession().getSchema().orElseThrow()));
        queryRunner.execute("GRANT SELECT ON tpch.\"*\".\"*\" TO ROLE %s WITH GRANT OPTION".formatted(ACCOUNT_ADMIN));

        // Allows for tests in BaseIcebergMaterializedViewTest which use non_existent to check for not found
        galaxyTestHelper.getAccountClient()
                .grantFunctionPrivilege(new TestingAccountClient.GrantDetails(Privilege.CREATE_TABLE,
                        galaxyTestHelper.getAccountClient().getAdminRoleId(),
                        ALLOW,
                        false,
                        new SchemaId(galaxyTestHelper.getAccountClient().getOrCreateCatalog(TEST_CATALOG), "non_existent")));
        return queryRunner;
    }

    @Override
    protected boolean isObjectStore()
    {
        return true;
    }

    @Override
    protected String getStorageMetadataLocation(String materializedViewName)
    {
        Table table = metastore.getMetastore().getTable("default", materializedViewName).orElseThrow();
        return table.parameters().get(METADATA_LOCATION_PROP);
    }

    @Test
    public void testDropMaterializedViewData()
    {
        String schemaForDroppedMaterializedView = "storage_schema_for_drop_" + randomNameSuffix();

        getQueryRunner().execute("CREATE TABLE test_drop_mv_data_table(value INT)");
        getQueryRunner().execute("INSERT INTO  test_drop_mv_data_table(value) VALUES 1, 2, 3, 4, 5");
        getQueryRunner().execute("CREATE SCHEMA %s".formatted(schemaForDroppedMaterializedView));
        assertQueryFails("CREATE MATERIALIZED VIEW test_drop_mv_data_materialized_view "
                + " WITH ( storage_schema = '%s' ) ".formatted(schemaForDroppedMaterializedView)
                + " AS SELECT value as top_two_value FROM test_drop_mv_data_table ORDER BY value DESC LIMIT 2",
                "Materialized view property 'storage_schema' is not supported when hiding materialized view storage tables is enabled");

        assertUpdate("DROP TABLE test_drop_mv_data_table");
        getQueryRunner().execute("DROP SCHEMA %s".formatted(schemaForDroppedMaterializedView));
    }

    // TODO move test to BaseObjectStoreMaterializedViewTest
    @Test
    public void testMaterializedViewPermissions()
    {
        String materializedViewName = "test_materialized_view_permission_" + randomNameSuffix();
        String tableName = "test_materialized_table_" + randomNameSuffix();

        computeActual("CREATE TABLE " + tableName + " (a varchar)");
        assertUpdate("INSERT INTO " + tableName + " VALUES '42'", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES '45'", 1);
        computeActual("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT * FROM " + tableName);
        assertQuery("SELECT * FROM " + materializedViewName, "VALUES 42, 45");
        assertQueryFails(getPublicRoleSessionWithQueryCatalogs(),
                "SELECT * FROM " + format("%s.%s.%s", TEST_CATALOG, "default", materializedViewName), "Access Denied: Cannot select from columns.*");
        assertQueryFails(getPublicRoleSessionWithQueryCatalogs(),
                "SELECT 1 FROM " + format("%s.%s.%s", TEST_CATALOG, "default", materializedViewName), "Access Denied: Cannot select from columns.*");
    }

    // TODO move test to BaseObjectStoreMaterializedViewTest
    @Test
    public void testRefreshedMaterializedViewPermissions()
    {
        String materializedViewName = "test_refreshed_materialized_view_permission_" + randomNameSuffix();
        String tableName = "test_materialized_table_" + randomNameSuffix();

        computeActual("CREATE TABLE " + tableName + " (a varchar)");
        assertUpdate("INSERT INTO " + tableName + " VALUES '42'", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES '45'", 1);
        computeActual("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT * FROM " + tableName);
        computeActual("REFRESH MATERIALIZED VIEW " + materializedViewName);
        assertQuery("SELECT * FROM " + materializedViewName, "VALUES 42, 45");
        assertQueryFails(getPublicRoleSessionWithQueryCatalogs(),
                "SELECT * FROM " + format("%s.%s.%s", TEST_CATALOG, "default", materializedViewName), "Access Denied: Cannot select from columns.*");
        assertQueryFails(getPublicRoleSessionWithQueryCatalogs(),
                "SELECT 1 FROM " + format("%s.%s.%s", TEST_CATALOG, "default", materializedViewName), "Access Denied: Cannot select from columns.*");
    }

    // TODO move test to BaseObjectStoreMaterializedViewTest
    @Test
    public void testStorageSchemaPropertyGalaxyAccessControl()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        String viewName = "galaxy_storage_schema_test_view";

        assertThatThrownBy(() -> query(
                "CREATE MATERIALIZED VIEW " + viewName + " " +
                        "WITH (storage_schema = 'different_storage_schema') AS " +
                        "SELECT * FROM base_table1"))
                .hasMessageContaining("Access Denied: Cannot create materialized view iceberg.different_storage_schema.%s: Role accountadmin does not have the privilege CREATE_TABLE on the schema iceberg.different_storage_schema".formatted(viewName));
        assertThatThrownBy(() -> query("DESCRIBE " + viewName))
                .hasMessageContaining(format("'iceberg.%s.%s' does not exist", schemaName, viewName));
    }

    private Session getPublicRoleSessionWithQueryCatalogs()
    {
        return Session.builder(galaxyTestHelper.publicSession())
                .setQueryCatalogs(getSession().getQueryCatalogs())
                .build();
    }

    @Override
    protected TableMetadata getStorageTableMetadata(String materializedViewName)
    {
        Location metadataLocation = Location.of(getStorageMetadataLocation(materializedViewName));
        TrinoFileSystem fileSystem = getTrinoFileSystem();
        return TableMetadataParser.read(new ForwardingFileIo(fileSystem), metadataLocation.toString());
    }

    private TrinoFileSystem getTrinoFileSystem()
    {
        ConfigurationInitializer s3Initializer = new TrinoS3ConfigurationInitializer(new HiveS3Config()
                .setS3AwsAccessKey(ACCESS_KEY)
                .setS3AwsSecretKey(SECRET_KEY));
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(s3Initializer));
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, ImmutableSet.of());
        return new HdfsFileSystemFactory(new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication()), new TrinoHdfsFileSystemStats())
                .create(SESSION);
    }
}
