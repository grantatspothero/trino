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
import io.trino.sql.tree.ExplainType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

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
            throws Exception
    {
        galaxyTestHelper = null;
        minio = null;
    }

    // override to remove non-galaxy supported properties for Iceberg materialized views
    @Override
    @Test
    public void testShowCreate()
    {
        String schema = getSession().getSchema().orElseThrow();

        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_with_property " +
                "WITH (\n" +
                "   partitioning = ARRAY['_date'],\n" +
                "   orc_bloom_filter_columns = ARRAY['_date'],\n" +
                "   orc_bloom_filter_fpp = 0.1) AS " +
                "SELECT _bigint, _date FROM base_table1");
        assertQuery("SELECT COUNT(*) FROM materialized_view_with_property", "VALUES 6");

        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW materialized_view_with_property"))
                .matches(
                        "\\QCREATE MATERIALIZED VIEW iceberg." + schema + ".materialized_view_with_property\n" +
                                "WITH (\n" +
                                "   orc_bloom_filter_columns = ARRAY['_date'],\n" +
                                "   orc_bloom_filter_fpp = 1E-1,\n" +
                                "   partitioning = ARRAY['_date'],\n" +
                                "   storage_schema = '" + schema + "'\n" +
                                ") AS\n" +
                                "SELECT\n" +
                                "  _bigint\n" +
                                ", _date\n" +
                                "FROM\n" +
                                "  base_table1");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_with_property");
    }

    // override to remove non-galaxy supported properties for Iceberg materialized views
    @Override
    @Test
    public void testSqlFeatures()
    {
        String schema = getSession().getSchema().orElseThrow();

        // Materialized views to test SQL features
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_window WITH (partitioning = ARRAY['_date']) as select _date, " +
                "sum(_bigint) OVER (partition by _date order by _date) as sum_ints from base_table1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_window", 6);
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_union WITH (partitioning = ARRAY['_date']) as " +
                "select _date, count(_date) as num_dates from base_table1 group by 1 union " +
                "select _date, count(_date) as num_dates from base_table2 group by 1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_union", 5);
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_subquery WITH (partitioning = ARRAY['_date']) as " +
                "select _date, count(_date) as num_dates from base_table1 where _date = (select max(_date) from base_table2) group by 1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_subquery", 1);

        // This set of tests intend to test various SQL features in the context of materialized views. It also tests commands pertaining to materialized views.
        String plan = getExplainPlan("SELECT * from materialized_view_window", ExplainType.Type.IO);
        assertThat(plan).doesNotContain("base_table1");
        plan = getExplainPlan("SELECT * from materialized_view_union", ExplainType.Type.IO);
        assertThat(plan).doesNotContain("base_table1");
        plan = getExplainPlan("SELECT * from materialized_view_subquery", ExplainType.Type.IO);
        assertThat(plan).doesNotContain("base_table1");

        String qualifiedMaterializedViewName = "iceberg." + schema + ".materialized_view_window";
        assertQueryFails("show create view  materialized_view_window",
                "line 1:1: Relation '" + qualifiedMaterializedViewName + "' is a materialized view, not a view");

        assertThat((String) computeScalar("show create materialized view materialized_view_window"))
                .matches("\\QCREATE MATERIALIZED VIEW " + qualifiedMaterializedViewName + "\n" +
                        "WITH (\n" +
                        "   partitioning = ARRAY['_date'],\n" +
                        "   storage_schema = '" + schema + "'\n" +
                        ") AS\n" +
                        "SELECT\n" +
                        "  _date\n" +
                        ", sum(_bigint) OVER (PARTITION BY _date ORDER BY _date ASC) sum_ints\n" +
                        "FROM\n" +
                        "  base_table1");

        assertQueryFails("INSERT INTO materialized_view_window VALUES (0, '2019-09-08'), (1, DATE '2019-09-09'), (2, DATE '2019-09-09')",
                "Inserting into materialized views is not supported");

        MaterializedResult result = computeActual("explain (type logical) refresh materialized view materialized_view_window");
        assertEquals(result.getRowCount(), 1);
        result = computeActual("explain (type distributed) refresh materialized view materialized_view_window");
        assertEquals(result.getRowCount(), 1);
        result = computeActual("explain (type validate) refresh materialized view materialized_view_window");
        assertEquals(result.getRowCount(), 1);
        result = computeActual("explain (type io) refresh materialized view materialized_view_window");
        assertEquals(result.getRowCount(), 1);
        result = computeActual("explain analyze refresh materialized view materialized_view_window");
        assertEquals(result.getRowCount(), 1);

        assertUpdate("DROP MATERIALIZED VIEW materialized_view_window");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_union");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_subquery");
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
