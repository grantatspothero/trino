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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_DATABASES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_TABLES_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_VIEWS_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE_WITH_PARAMETER;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Similar to {@link io.trino.plugin.hive.metastore.thrift.TestHiveMetastoreMetadataQueriesAccessOperations},
 * but it doesn't extend the class because the access count differs
 */
@Test(singleThreaded = true) // metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestObjectStoreGalaxyMetastoreMetadataQueriesAccessOperations
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "objectstore";
    private static final String SCHEMA_NAME = "test_schema";

    private CountingAccessHiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path schemaDirectory = createTempDirectory(null);
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());

        TestingGalaxyMetastore galaxyMetastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));
        metastore = new CountingAccessHiveMetastore(new GalaxyHiveMetastore(galaxyMetastore.getMetastore(), HDFS_ENVIRONMENT, schemaDirectory.toUri().toString(), new GalaxyHiveMetastoreConfig().isBatchMetadataFetch()));

        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> galaxyCockroachContainer));

        Map<String, String> properties = createObjectStoreProperties(
                new ObjectStoreConfig().getTableType(),
                ImmutableMap.<String, String>builder()
                        .put("galaxy.location-security.enabled", "false")
                        .put("galaxy.catalog-id", "c-1234567890")
                        .put("galaxy.account-url", "https://localhost:1234")
                        .buildOrThrow(),
                "galaxy",
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("DELTA__delta.enable-non-concurrent-writes", "true")
                        // Galaxy uses metastore cache by default, but disabling it for verifying the raw behavior
                        .put("HIVE__hive.metastore-cache-ttl", "0s")
                        .put("DELTA__hive.metastore-cache-ttl", "0s")
                        .put("HUDI__hive.metastore-cache-ttl", "0s")
                        .buildOrThrow());
        properties = Maps.filterEntries(
                properties,
                entry -> !entry.getKey().equals("HIVE__hive.metastore") &&
                        !entry.getKey().equals("ICEBERG__iceberg.catalog.type") &&
                        !entry.getKey().equals("DELTA__hive.metastore") &&
                        !entry.getKey().equals("HUDI__hive.metastore"));

        DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder(CATALOG_NAME, SCHEMA_NAME)
                .setAccountClient(testingAccountFactory.createAccountClient())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new TestingObjectStorePlugin(metastore, new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS)))
                .addCatalog(CATALOG_NAME, "galaxy_objectstore", properties)
                .build();
        queryRunner.execute("CREATE SCHEMA %s.%s WITH (location = '%s')".formatted(CATALOG_NAME, SCHEMA_NAME, schemaDirectory.toUri().toString()));
        return queryRunner;
    }

    @Test
    public void testSelectSchemasWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.schemata", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectSchemasWithFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.schemata WHERE schema_name = 'information_schema'", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas WHERE table_schem = 'information_schema'", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectSchemasWithLikeOverSchemaName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.schemata WHERE schema_name LIKE 'test%'", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas WHERE table_schem LIKE 'test%'", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectTablesWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectTablesWithFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectTablesWithFilterBySchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectTablesWithLikeOverSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectTablesWithFilterByTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectTablesWithLikeOverTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectViewsWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
    }

    @Test
    public void testSelectViewsWithFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectViewsWithFilterBySchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema = 'test_schema_0'", ImmutableMultiset.of(GET_ALL_VIEWS_FROM_DATABASE));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectViewsWithLikeOverSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectViewsWithFilterByTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLE)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectViewsWithLikeOverTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 2)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectColumnsWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 3)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, 3)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 3)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, 3)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectColumnsFilterBySchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, 3)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, 3)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test\\_schema\\_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, 3)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test_schema_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, 3)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByTableName()
    {
        metastore.resetCounters();
        computeActual("SELECT * FROM information_schema.columns WHERE table_name = 'test_table_0'");
        Multiset<CountingAccessHiveMetastore.Method> invocations = metastore.getMethodInvocations();

        assertThat(invocations.count(GET_TABLE)).as("GET_TABLE invocations")
                .isEqualTo(2);
        invocations = HashMultiset.create(invocations);
        invocations.elementSet().remove(GET_TABLE);

        assertThat(invocations).as("invocations except of GET_TABLE")
                .isEqualTo(ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .build());

        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByColumn()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE column_name = 'name'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 3)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, 3)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE column_name = 'name'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 3)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, 3)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverColumn()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE column_name LIKE 'n%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE column_name LIKE 'n%'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 3)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, 3)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .add(GET_TABLE_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByTableAndSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema = 'test_schema_0' AND table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem = 'test_schema_0' AND table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test\\_schema\\_0' ESCAPE '\\' AND table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test_schema_0' ESCAPE '\\' AND table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .build());
    }

    private void assertMetastoreInvocations(@Language("SQL") String query, Multiset<?> expectedInvocations)
    {
        CountingAccessHiveMetastoreUtil.assertMetastoreInvocations(metastore, getQueryRunner(), getQueryRunner().getDefaultSession(), query, expectedInvocations);
    }
}
