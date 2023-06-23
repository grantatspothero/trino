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
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import io.trino.hdfs.TrinoFileSystemCache;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITIONS_BY_NAMES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITION_NAMES_BY_FILTER;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITION_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.REPLACE_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.UPDATE_PARTITION_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.UPDATE_TABLE_STATISTICS;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.nio.file.Files.createTempDirectory;

// TODO Investigate why there are many invocations in some tests
@Test(singleThreaded = true) // metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestObjectStoreMetastoreAccessOperations
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "objectstore";
    private static final String SCHEMA_NAME = "test_schema";

    private CountingAccessHiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        closeAfterClass(TrinoFileSystemCache.INSTANCE::closeAll);

        Path schemaDirectory = createTempDirectory(null);
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());

        TestingGalaxyMetastore galaxyMetastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));
        metastore = new CountingAccessHiveMetastore(new GalaxyHiveMetastore(galaxyMetastore.getMetastore(), HDFS_ENVIRONMENT, schemaDirectory.toUri().toString()));

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
                ImmutableMap.of("DELTA__delta.enable-non-concurrent-writes", "true"));
        properties = Maps.filterEntries(
                properties,
                entry -> !entry.getKey().equals("HIVE__hive.metastore") &&
                        !entry.getKey().equals("ICEBERG__iceberg.catalog.type") &&
                        !entry.getKey().equals("DELTA__hive.metastore") &&
                        !entry.getKey().equals("HUDI__hive.metastore"));

        DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder(CATALOG_NAME, SCHEMA_NAME)
                .setAccountClient(testingAccountFactory.createAccountClient())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new TestingObjectStorePlugin(metastore))
                .addCatalog(CATALOG_NAME, "galaxy_objectstore", properties)
                .build();
        queryRunner.execute("CREATE SCHEMA %s.%s WITH (location = '%s')".formatted(CATALOG_NAME, SCHEMA_NAME, schemaDirectory.toUri().toString()));
        return queryRunner;
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        listTables("BASE TABLE").forEach(tableName -> query("DROP TABLE " + tableName));
        listTables("VIEW").forEach(tableName -> query("DROP VIEW " + tableName));
    }

    private Stream<Object> listTables(String tableType)
    {
        return computeActual("SELECT table_name FROM information_schema.tables " +
                "WHERE table_type = '" + tableType + "' AND " +
                "table_schema = '" + SCHEMA_NAME + "'").getOnlyColumn();
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testCreateTable(TableType type)
    {
        assertMetastoreInvocations("CREATE TABLE test_create(id VARCHAR, age INT) WITH (type = '" + type + "')",
                ImmutableMultiset.builder()
                        .addCopies(GET_DATABASE, occurrences(0, 1, 0).get(type))
                        .add(CREATE_TABLE)
                        .addCopies(GET_TABLE, occurrences(2, 1, 2).get(type))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(1, 0, 0).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testCreateTableAsSelect(TableType type)
    {
        assertMetastoreInvocations("CREATE TABLE test_ctas WITH (type = '" + type + "') AS SELECT 1 AS age",
                ImmutableMultiset.builder()
                        .add(CREATE_TABLE)
                        .addCopies(GET_TABLE, occurrences(3, 5, 2).get(type))
                        .addCopies(GET_DATABASE, occurrences(0, 1, 0).get(type))
                        .addCopies(REPLACE_TABLE, occurrences(0, 1, 0).get(type))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(1, 0, 0).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelect(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_from(id VARCHAR, age INT) WITH (type = '" + type + "')");

        assertMetastoreInvocations("SELECT * FROM test_select_from",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(4, 2, 3).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelectPartitionedTable(TableType type)
    {
        String partitionProperty = type == ICEBERG ? "partitioning" : "partitioned_by";
        assertUpdate("CREATE TABLE test_select_partition WITH (" + partitionProperty + " = ARRAY['part'], type = '" + type + "') AS SELECT 1 AS data, 10 AS part", 1);

        assertMetastoreInvocations("SELECT * FROM test_select_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(5, 2, 3).get(type))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(1, 0, 0).get(type))
                        .addCopies(GET_PARTITIONS_BY_NAMES, occurrences(1, 0, 0).get(type))
                        .build());

        assertUpdate("INSERT INTO test_select_partition SELECT 2 AS data, 20 AS part", 1);
        assertMetastoreInvocations("SELECT * FROM test_select_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(2, 1, 1).get(type))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(1, 0, 0).get(type))
                        .addCopies(GET_PARTITIONS_BY_NAMES, occurrences(1, 0, 0).get(type))
                        .build());

        // Specify a specific partition
        assertMetastoreInvocations("SELECT * FROM test_select_partition WHERE part = 10",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(2, 1, 1).get(type))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(1, 0, 0).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelectWithFilter(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_from_where WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("SELECT * FROM test_select_from_where WHERE age = 2",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, new Occurrences(4, 2, 3).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelectFromView(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_view_table(id VARCHAR, age INT) WITH (type = '" + type + "')");
        assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

        assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 3)
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelectFromViewWithFilter(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_view_where_table WITH (type = '" + type + "') AS SELECT 2 AS age", 1);
        assertUpdate("CREATE VIEW test_select_view_where_view AS SELECT age FROM test_select_view_where_table");

        assertMetastoreInvocations("SELECT * FROM test_select_view_where_view WHERE age = 2",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 3)
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testJoin(TableType type)
    {
        assertUpdate("CREATE TABLE test_join_t1 WITH (type = '" + type + "') AS SELECT 2 AS age, 'id1' AS id", 1);
        assertUpdate("CREATE TABLE test_join_t2 WITH (type = '" + type + "') AS SELECT 'name1' AS name, 'id1' AS id", 1);

        assertMetastoreInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(8, 4, 6).get(type))
                        .addCopies(GET_TABLE_STATISTICS, occurrences(2, 0, 0).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelfJoin(TableType type)
    {
        assertUpdate("CREATE TABLE test_self_join_table WITH (type = '" + type + "') AS SELECT 2 AS age, 0 parent, 3 AS id", 1);

        assertMetastoreInvocations("SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(4, 2, 3).get(type))
                        .addCopies(GET_TABLE_STATISTICS, occurrences(1, 0, 0).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testExplainSelect(TableType type)
    {
        assertUpdate("CREATE TABLE test_explain WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("EXPLAIN SELECT * FROM test_explain",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(4, 2, 3).get(type))
                        .addCopies(GET_TABLE_STATISTICS, occurrences(1, 0, 0).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testShowStatsForTable(TableType type)
    {
        assertUpdate("CREATE TABLE test_show_stats WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("SHOW STATS FOR test_show_stats",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(4, 2, 3).get(type))
                        .addCopies(GET_TABLE_STATISTICS, occurrences(1, 0, 0).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testShowStatsForTableWithFilter(TableType type)
    {
        assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter where age >= 2)",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(4, 2, 2).get(type))
                        .addCopies(GET_TABLE_STATISTICS, occurrences(1, 1, 1).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testAnalyze(TableType type)
    {
        assertUpdate("CREATE TABLE test_analyze WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertMetastoreInvocations("ANALYZE test_analyze",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(4, 4, 3).get(type))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(1, 0, 0).get(type))
                        .addCopies(REPLACE_TABLE, occurrences(0, 1, 0).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testAnalyzePartitionedTable(TableType type)
    {
        String partitionProperty = type == ICEBERG ? "partitioning" : "partitioned_by";
        assertUpdate("CREATE TABLE test_analyze_partition WITH (" + partitionProperty + " = ARRAY['part'], type = '" + type + "') AS SELECT 1 AS data, 10 AS part", 1);

        assertMetastoreInvocations("ANALYZE test_analyze_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(5, 4, 3).get(type))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(1, 0, 0).get(type))
                        .addCopies(GET_PARTITIONS_BY_NAMES, occurrences(1, 0, 0).get(type))
                        .addCopies(GET_PARTITION_STATISTICS, occurrences(1, 0, 0).get(type))
                        .addCopies(UPDATE_PARTITION_STATISTICS, occurrences(1, 0, 0).get(type))
                        .addCopies(REPLACE_TABLE, occurrences(0, 1, 0).get(type))
                        .build());

        assertUpdate("INSERT INTO test_analyze_partition SELECT 2 AS data, 20 AS part", 1);

        assertMetastoreInvocations("ANALYZE test_analyze_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(1, 3, 0).get(type))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(1, 0, 0).get(type))
                        .addCopies(GET_PARTITIONS_BY_NAMES, occurrences(1, 0, 0).get(type))
                        .addCopies(GET_PARTITION_STATISTICS, occurrences(1, 0, 0).get(type))
                        .addCopies(UPDATE_PARTITION_STATISTICS, occurrences(1, 0, 0).get(type))
                        .addCopies(REPLACE_TABLE, occurrences(0, 1, 0).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testDropStats(TableType type)
    {
        assertUpdate("CREATE TABLE drop_stats WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        String dropStats = switch (type) {
            case HIVE -> "CALL system.drop_stats('test_schema', 'drop_stats')";
            case ICEBERG -> "ALTER TABLE drop_stats EXECUTE drop_extended_stats";
            case DELTA -> "CALL system.drop_extended_stats('test_schema', 'drop_stats')";
            default -> throw new IllegalArgumentException("Unexpected table type: " + type);
        };
        assertMetastoreInvocations(dropStats,
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(1, 4, 1).get(type))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(1, 0, 0).get(type))
                        .addCopies(REPLACE_TABLE, occurrences(0, 1, 0).get(type))
                        .build());
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testDropStatsPartitionedTable(TableType type)
    {
        String partitionProperty = type == ICEBERG ? "partitioning" : "partitioned_by";
        assertUpdate("CREATE TABLE drop_stats_partition WITH (" + partitionProperty + " = ARRAY['part'], type = '" + type + "') AS SELECT 1 AS data, 10 AS part", 1);

        String dropStats = switch (type) {
            case HIVE -> "CALL system.drop_stats('test_schema', 'drop_stats_partition')";
            case ICEBERG -> "ALTER TABLE drop_stats_partition EXECUTE drop_extended_stats";
            case DELTA -> "CALL system.drop_extended_stats('test_schema', 'drop_stats_partition')";
            default -> throw new IllegalArgumentException("Unexpected table type: " + type);
        };
        assertMetastoreInvocations(dropStats,
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(2, 4, 1).get(type))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(1, 0, 0).get(type))
                        .addCopies(UPDATE_PARTITION_STATISTICS, occurrences(1, 0, 0).get(type))
                        .addCopies(REPLACE_TABLE, occurrences(0, 1, 0).get(type))
                        .build());

        assertUpdate("INSERT INTO drop_stats_partition SELECT 2 AS data, 20 AS part", 1);

        assertMetastoreInvocations(dropStats,
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(1, 2, 0).get(type))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(1, 0, 0).get(type))
                        .addCopies(UPDATE_PARTITION_STATISTICS, occurrences(2, 0, 0).get(type))
                        .addCopies(REPLACE_TABLE, occurrences(0, 0, 0).get(type))
                        .build());
    }

    @DataProvider
    public Object[][] tableTypeDataProvider()
    {
        return Arrays.stream(TableType.values())
                .filter(type -> type != HUDI) // TODO Remove this filter once Hudi connector supports creating tables
                .collect(toDataProvider());
    }

    private void assertMetastoreInvocations(@Language("SQL") String query, Multiset<?> expectedInvocations)
    {
        CountingAccessHiveMetastoreUtil.assertMetastoreInvocations(metastore, getQueryRunner(), getQueryRunner().getDefaultSession(), query, expectedInvocations);
    }

    private static Occurrences occurrences(int hive, int iceberg, int delta)
    {
        return new Occurrences(hive, iceberg, delta);
    }

    @SuppressWarnings("unused")
    private record Occurrences(int hive, int iceberg, int delta)
    {
        int get(TableType type)
        {
            return switch (type) {
                case HIVE -> hive;
                case ICEBERG -> iceberg;
                case DELTA -> delta;
                default -> throw new IllegalArgumentException("Unexpected table type: " + type);
            };
        }
    }
}
