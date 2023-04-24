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
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.trino.hdfs.TrinoFileSystemCache;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.BaseIcebergMaterializedViewTest;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.security.galaxy.GalaxyTestHelper;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestObjectStoreIcebergConnectorMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private static final String TEST_CATALOG = "iceberg";
    private static final String TEST_BUCKET = "test-bucket";

    private GalaxyTestHelper galaxyTestHelper;
    private String schemaDirectory;
    private MinioStorage minio;

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

        closeAfterClass(TrinoFileSystemCache.INSTANCE::closeAll);
        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> false));

        minio = closeAfterClass(new MinioStorage(TEST_BUCKET));
        minio.start();
        schemaDirectory = minio.getS3Url() + "/" + storageSchemaName;
        TestingGalaxyMetastore metastore = closeAfterClass(new TestingGalaxyMetastore(galaxyTestHelper.getCockroach()));

        Map<String, String> properties = createObjectStoreProperties(
                ICEBERG,
                ImmutableMap.<String, String>builder()
                        .putAll(locationSecurityServer.getClientConfig())
                        .put("galaxy.catalog-id", "c-1234567890")
                        .buildOrThrow(),
                metastore.getMetastoreConfig(minio.getS3Url()),
                minio.getHiveS3Config());
        DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder(TEST_CATALOG, "default")
                .setAccountClient(galaxyTestHelper.getAccountClient())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new ObjectStorePlugin())
                .addCatalog(TEST_CATALOG, "galaxy_objectstore", properties)
                .addPlugin(new TpchPlugin())
                .addCatalog("tpch", "tpch", ImmutableMap.of())
                .build();
        queryRunner.execute("CREATE SCHEMA %s.%s".formatted(TEST_CATALOG, queryRunner.getDefaultSession().getSchema().orElseThrow()));
        queryRunner.execute("GRANT SELECT ON tpch.\"*\".\"*\" TO ROLE %s WITH GRANT OPTION".formatted(ACCOUNT_ADMIN));
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        galaxyTestHelper = null; // closed by closeAfterClass
        minio = null; // closed by closeAfterClass
    }

    @Override
    protected boolean isObjectStore()
    {
        return true;
    }

    @Test
    public void testDropMaterializedViewData()
    {
        String schemaForDroppedMaterializedView = "storage_schema_for_drop_" + randomNameSuffix();

        getQueryRunner().execute("CREATE TABLE test_drop_mv_data_table(value INT)");
        getQueryRunner().execute("INSERT INTO  test_drop_mv_data_table(value) VALUES 1, 2, 3, 4, 5");
        getQueryRunner().execute("CREATE SCHEMA %s".formatted(schemaForDroppedMaterializedView));
        getQueryRunner().execute("CREATE MATERIALIZED VIEW test_drop_mv_data_materialized_view "
                + " WITH ( storage_schema = '%s' ) ".formatted(schemaForDroppedMaterializedView)
                + " AS SELECT value as top_two_value FROM test_drop_mv_data_table ORDER BY value DESC LIMIT 2");

        getQueryRunner().execute("REFRESH MATERIALIZED VIEW  test_drop_mv_data_materialized_view");
        MinioClient client = MinioClient.builder()
                .endpoint(minio.getEndpoint())
                .credentials(MinioStorage.ACCESS_KEY, MinioStorage.SECRET_KEY)
                .build();
        assertThat(client.listObjects(ListObjectsArgs.builder()
                .recursive(true)
                .bucket(TEST_BUCKET)
                .prefix(schemaForDroppedMaterializedView)
                .build())).isNotEmpty();
        getQueryRunner().execute("DROP MATERIALIZED VIEW test_drop_mv_data_materialized_view");
        assertThat(client.listObjects(ListObjectsArgs.builder()
                .recursive(true)
                .bucket(TEST_BUCKET)
                .prefix(schemaForDroppedMaterializedView)
                .build())).isEmpty();

        getQueryRunner().execute("DROP SCHEMA %s".formatted(schemaForDroppedMaterializedView));
    }
}
