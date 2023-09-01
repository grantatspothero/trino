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
import io.trino.Session;
import io.trino.filesystem.TrackingFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.TrinoFileSystemCache;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.objectstore.ObjectStoreConfig.InformationSchemaQueriesAcceleration;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DataProviders;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_EXISTS;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_CREATE;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_CREATE_OR_OVERWRITE;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_LOCATION;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_DATABASES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_TABLES_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_VIEWS_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITIONS_BY_NAMES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITION_NAMES_BY_FILTER;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITION_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLES_WITH_PARAMETER;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.REPLACE_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.STREAM_TABLES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.UPDATE_PARTITION_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.UPDATE_TABLE_STATISTICS;
import static io.trino.plugin.hive.util.MultisetAssertions.assertMultisetsEqual;
import static io.trino.plugin.objectstore.ObjectStoreSessionProperties.INFORMATION_SCHEMA_QUERIES_ACCELERATION;
import static io.trino.plugin.objectstore.TestObjectStoreFileAndMetastoreAccessOperations.FileType.CDF_DATA;
import static io.trino.plugin.objectstore.TestObjectStoreFileAndMetastoreAccessOperations.FileType.DATA;
import static io.trino.plugin.objectstore.TestObjectStoreFileAndMetastoreAccessOperations.FileType.LAST_CHECKPOINT;
import static io.trino.plugin.objectstore.TestObjectStoreFileAndMetastoreAccessOperations.FileType.MANIFEST;
import static io.trino.plugin.objectstore.TestObjectStoreFileAndMetastoreAccessOperations.FileType.METADATA_JSON;
import static io.trino.plugin.objectstore.TestObjectStoreFileAndMetastoreAccessOperations.FileType.SNAPSHOT;
import static io.trino.plugin.objectstore.TestObjectStoreFileAndMetastoreAccessOperations.FileType.STATS;
import static io.trino.plugin.objectstore.TestObjectStoreFileAndMetastoreAccessOperations.FileType.TRANSACTION_LOG_JSON;
import static io.trino.plugin.objectstore.TestObjectStoreFileAndMetastoreAccessOperations.FileType.TRINO_EXTENDED_STATS_JSON;
import static io.trino.plugin.objectstore.TestObjectStoreFileAndMetastoreAccessOperations.TableType.ICEBERG;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;

// TODO Investigate why there are many invocations in some tests
@Test(singleThreaded = true) // metastore and filesystem invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestObjectStoreFileAndMetastoreAccessOperations
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 10;
    private static final String CATALOG_NAME = "objectstore";
    private static final String SCHEMA_NAME = "test_schema";

    private CountingAccessHiveMetastore metastore;
    private TrackingFileSystemFactory trackingFileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        closeAfterClass(TrinoFileSystemCache.INSTANCE::closeAll);

        Path schemaDirectory = createTempDirectory(null);
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());

        TestingGalaxyMetastore galaxyMetastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));
        metastore = new CountingAccessHiveMetastore(new GalaxyHiveMetastore(galaxyMetastore.getMetastore(), HDFS_ENVIRONMENT, schemaDirectory.toUri().toString(), new GalaxyHiveMetastoreConfig().isBatchMetadataFetch()));
        trackingFileSystemFactory = new TrackingFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS));

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
                ImmutableMap.of(
                        // Disable caching for test method isolation
                        "HIVE__hive.metastore-cache-ttl", "0s",
                        "DELTA__hive.metastore-cache-ttl", "0s",
                        "HUDI__hive.metastore-cache-ttl", "0s",
                        // Enable Delta table creation
                        "DELTA__delta.enable-non-concurrent-writes", "true"));
        properties = Maps.filterEntries(
                properties,
                entry -> !entry.getKey().equals("HIVE__hive.metastore") &&
                        !entry.getKey().equals("ICEBERG__iceberg.catalog.type") &&
                        !entry.getKey().equals("DELTA__hive.metastore") &&
                        !entry.getKey().equals("HUDI__hive.metastore"));

        DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder(CATALOG_NAME, SCHEMA_NAME)
                .setAccountClient(testingAccountFactory.createAccountClient())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new TestingObjectStorePlugin(metastore, trackingFileSystemFactory))
                .addCatalog(CATALOG_NAME, "galaxy_objectstore", properties)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .build();
        queryRunner.execute("CREATE SCHEMA %s.%s WITH (location = '%s')".formatted(CATALOG_NAME, SCHEMA_NAME, schemaDirectory.toUri().toString()));
        return queryRunner;
    }

    @AfterMethod(alwaysRun = true)
    public void cleanUp()
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
        assertInvocations("CREATE TABLE test_create(id VARCHAR, age INT) WITH (type = '" + type + "')",
                ImmutableMultiset.builder()
                        .addCopies(GET_DATABASE, 1)
                        .add(CREATE_TABLE)
                        .add(GET_TABLE)
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", OUTPUT_FILE_LOCATION))
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", OUTPUT_FILE_CREATE))
                            .addCopies(new FileOperation(SNAPSHOT, "snap-1.avro", OUTPUT_FILE_LOCATION), 2)
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", OUTPUT_FILE_CREATE))
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testCreateTableAsSelect(TableType type)
    {
        assertInvocations("CREATE TABLE test_ctas WITH (type = '" + type + "') AS SELECT 1 AS age",
                ImmutableMultiset.builder()
                        .add(CREATE_TABLE)
                        .addCopies(GET_TABLE, occurrences(type, 2, 5, 1))
                        .addCopies(GET_DATABASE, 1)
                        .addCopies(REPLACE_TABLE, occurrences(type, 0, 1, 0))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of(
                            new FileOperation(DATA, "no partition", OUTPUT_FILE_CREATE));
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", OUTPUT_FILE_LOCATION))
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", OUTPUT_FILE_LOCATION))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .addCopies(new FileOperation(SNAPSHOT, "snap-1.avro", OUTPUT_FILE_LOCATION), 2)
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", OUTPUT_FILE_LOCATION))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(STATS, "", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(MANIFEST, "", OUTPUT_FILE_LOCATION))
                            .add(new FileOperation(MANIFEST, "", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(DATA, "no partition", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_EXISTS))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", OUTPUT_FILE_CREATE))
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelectFromEmpty(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_from(id VARCHAR, age INT) WITH (type = '" + type + "')");

        assertInvocations("SELECT * FROM test_select_from",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 2, 2, 3))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelectPartitionedTable(TableType type)
    {
        String partitionProperty = type == ICEBERG ? "partitioning" : "partitioned_by";
        assertUpdate("CREATE TABLE test_select_partition WITH (" + partitionProperty + " = ARRAY['part'], type = '" + type + "') AS SELECT 1 AS data, 10 AS part", 1);

        assertInvocations("SELECT * FROM test_select_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 3, 2, 3))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(type, 1, 0, 0))
                        .addCopies(GET_PARTITIONS_BY_NAMES, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of(
                            new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM));
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                });

        assertUpdate("INSERT INTO test_select_partition SELECT 2 AS data, 20 AS part", 1);
        assertInvocations("SELECT * FROM test_select_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 3, 2, 3))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(type, 1, 0, 0))
                        .addCopies(GET_PARTITIONS_BY_NAMES, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(METADATA_JSON, "00003.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });

        // Specify a specific partition
        assertInvocations("SELECT * FROM test_select_partition WHERE part = 10",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 3, 2, 3))
                        .addCopies(GET_PARTITIONS_BY_NAMES, occurrences(type, 1, 0, 0))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(METADATA_JSON, "00003.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelectWithFilter(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_from_where WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertInvocations("SELECT * FROM test_select_from_where WHERE age = 2",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 2, 2, 3))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelectFromView(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_view_table(id VARCHAR, age INT) WITH (type = '" + type + "')");
        assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

        assertInvocations("SELECT * FROM test_select_view_view",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 4, 4, 5))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelectFromViewWithFilter(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_view_where_table WITH (type = '" + type + "') AS SELECT 2 AS age", 1);
        assertUpdate("CREATE VIEW test_select_view_where_view AS SELECT age FROM test_select_view_where_table");

        assertInvocations("SELECT * FROM test_select_view_where_view WHERE age = 2",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 4, 4, 5))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testJoin(TableType type)
    {
        assertUpdate("CREATE TABLE test_join_t1 WITH (type = '" + type + "') AS SELECT 2 AS age, 'id1' AS id", 1);
        assertUpdate("CREATE TABLE test_join_t2 WITH (type = '" + type + "') AS SELECT 'name1' AS name, 'id1' AS id", 1);

        assertInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 4, 4, 6))
                        .addCopies(GET_TABLE_STATISTICS, occurrences(type, 2, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH), 2)
                            .addCopies(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM), 4)
                            .addCopies(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 6)
                            .addCopies(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 6)
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testSelfJoin(TableType type)
    {
        assertUpdate("CREATE TABLE test_self_join_table WITH (type = '" + type + "') AS SELECT 2 AS age, 0 parent, 3 AS id", 1);

        assertInvocations("SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 2, 2, 3))
                        .addCopies(GET_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM), 3)
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 6)
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 6)
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testExplainSelect(TableType type)
    {
        assertUpdate("CREATE TABLE test_explain WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertInvocations("EXPLAIN SELECT * FROM test_explain",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 2, 2, 3))
                        .addCopies(GET_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testShowStatsForTable(TableType type)
    {
        assertUpdate("CREATE TABLE test_show_stats WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertInvocations("SHOW STATS FOR test_show_stats",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 2, 2, 3))
                        .addCopies(GET_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 3)
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 3)
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testShowStatsForTableWithFilter(TableType type)
    {
        assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 AS age", 1);

        assertInvocations("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter where age >= 2)",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .add(GET_TABLE_STATISTICS)
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of();
                    case ICEBERG -> ImmutableMultiset.of();
                    case DELTA -> ImmutableMultiset.of();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testAnalyze(TableType type)
    {
        assertUpdate("CREATE TABLE test_analyze WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertInvocations("ANALYZE test_analyze",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 1, 4, 3))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .addCopies(REPLACE_TABLE, occurrences(type, 0, 1, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_LOCATION))
                            .add(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(STATS, "", OUTPUT_FILE_CREATE))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_EXISTS))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testAnalyzePartitionedTable(TableType type)
    {
        String partitionProperty = type == ICEBERG ? "partitioning" : "partitioned_by";
        assertUpdate("CREATE TABLE test_analyze_partition WITH (" + partitionProperty + " = ARRAY['part'], type = '" + type + "') AS SELECT 1 AS data, 10 AS part", 1);

        assertInvocations("ANALYZE test_analyze_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 2, 4, 3))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(type, 1, 0, 0))
                        .addCopies(GET_PARTITIONS_BY_NAMES, occurrences(type, 1, 0, 0))
                        .addCopies(GET_PARTITION_STATISTICS, occurrences(type, 1, 0, 0))
                        .addCopies(UPDATE_PARTITION_STATISTICS, occurrences(type, 1, 0, 0))
                        .addCopies(REPLACE_TABLE, occurrences(type, 0, 1, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_LOCATION))
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(STATS, "", OUTPUT_FILE_CREATE))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_EXISTS))
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                });

        assertUpdate("INSERT INTO test_analyze_partition SELECT 2 AS data, 20 AS part", 1);

        assertInvocations("ANALYZE test_analyze_partition",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 2, 4, 2))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(type, 1, 0, 0))
                        .addCopies(GET_PARTITIONS_BY_NAMES, occurrences(type, 1, 0, 0))
                        .addCopies(GET_PARTITION_STATISTICS, occurrences(type, 1, 0, 0))
                        .addCopies(UPDATE_PARTITION_STATISTICS, occurrences(type, 1, 0, 0))
                        .addCopies(REPLACE_TABLE, occurrences(type, 0, 1, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .addCopies(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM), 2)
                            .add(new FileOperation(METADATA_JSON, "00005.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(METADATA_JSON, "00004.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00005.metadata.json", OUTPUT_FILE_LOCATION))
                            .add(new FileOperation(STATS, "", OUTPUT_FILE_CREATE))
                            .addCopies(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_EXISTS))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                });
    }

    @Test(dataProvider = "tableTypeDataProvider")
    public void testDropStats(TableType type)
    {
        assertUpdate("CREATE TABLE drop_stats WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        String dropStats = switch (type) {
            case HIVE -> "CALL system.drop_stats('test_schema', 'drop_stats')";
            case ICEBERG -> "ALTER TABLE drop_stats EXECUTE drop_extended_stats";
            case DELTA -> "CALL system.drop_extended_stats('test_schema', 'drop_stats')";
        };
        assertInvocations(dropStats,
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 1, 4, 1))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .addCopies(REPLACE_TABLE, occurrences(type, 0, 1, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_LOCATION))
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_CREATE))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_EXISTS))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .build();
                });
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
        };
        assertInvocations(dropStats,
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 2, 4, 1))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(type, 1, 0, 0))
                        .addCopies(UPDATE_PARTITION_STATISTICS, occurrences(type, 1, 0, 0))
                        .addCopies(REPLACE_TABLE, occurrences(type, 0, 1, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_LOCATION))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_EXISTS))
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });

        assertUpdate("INSERT INTO drop_stats_partition SELECT 2 AS data, 20 AS part", 1);

        assertInvocations(dropStats,
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 2, 3, 1))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(type, 1, 0, 0))
                        .addCopies(UPDATE_PARTITION_STATISTICS, occurrences(type, 2, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00003.metadata.json", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_EXISTS))
                            .build();
                });
    }

    @Test(dataProvider = "metadataTestDataProvider")
    public void testInformationSchemaColumns(TableType type, InformationSchemaQueriesAcceleration mode, int tables)
    {
        String catalog = getSession().getCatalog().orElseThrow();

        for (int i = 0; i < tables; i++) {
            assertUpdate("CREATE TABLE test_select_i_s_columns" + i + "(id VARCHAR, age INT) WITH (type = '" + type + "')");
            // Produce multiple snapshots and metadata files
            assertUpdate("INSERT INTO test_select_i_s_columns" + i + " VALUES ('abc', 11)", 1);
            assertUpdate("INSERT INTO test_select_i_s_columns" + i + " VALUES ('xyz', 12)", 1);

            assertUpdate("CREATE TABLE test_other_select_i_s_columns" + i + "(id varchar, age integer)"); // won't match the filter
        }

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, INFORMATION_SCHEMA_QUERIES_ACCELERATION, mode.toString())
                .build();

        assertInvocations(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'test_select_i_s_columns%'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, switch (mode) {
                            case NONE, V1, V2 -> 1;
                            case V3 -> 0;
                        })
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, switch (mode) {
                            case NONE, V1 -> 3;
                            case V2 -> 1;
                            case V3 -> 0;
                        })
                        .addCopies(GET_TABLES_WITH_PARAMETER, switch (mode) {
                            case NONE, V1, V2 -> 1;
                            case V3 -> 0;
                        })
                        .addCopies(STREAM_TABLES, switch (mode) {
                            case NONE, V1, V2 -> 0;
                            case V3 -> 1;
                        })
                        .addCopies(GET_TABLE, switch (mode) {
                            case NONE, V1 -> tables * 6;
                            case V2 -> occurrences(type, tables * 2, tables * 3, tables * 3);
                            case V3 -> 0; // 🎉
                        })
                        .build(),
                switch (type) {
                    case HIVE, ICEBERG -> ImmutableMultiset.of();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), tables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), tables)
                            .build();
                });

        InformationSchemaQueriesAcceleration defaultMode = new ObjectStoreConfig().getInformationSchemaQueriesAcceleration();
        if (mode != defaultMode) {
            // Correctness check (needs to be done after invocations check, otherwise invocations check could report on cached state)
            assertThat(query(session, "TABLE information_schema.columns"))
                    .matches(computeActual("TABLE information_schema.columns"));
        }
    }

    @Test(dataProvider = "metadataTestDataProvider")
    public void testSystemMetadataTableComments(TableType type, InformationSchemaQueriesAcceleration mode, int tables)
    {
        for (int i = 0; i < tables; i++) {
            assertUpdate("CREATE TABLE test_select_s_m_t_comments" + i + "(id VARCHAR, age INT) WITH (type = '" + type + "')");
            // Produce multiple snapshots and metadata files
            assertUpdate("INSERT INTO test_select_s_m_t_comments" + i + " VALUES ('abc', 11)", 1);
            assertUpdate("INSERT INTO test_select_s_m_t_comments" + i + " VALUES ('xyz', 12)", 1);

            assertUpdate("CREATE TABLE test_other_select_s_m_t_comments" + i + "(id varchar, age integer)"); // won't match the filter
        }

        String catalog = getSession().getCatalog().orElseThrow();

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, INFORMATION_SCHEMA_QUERIES_ACCELERATION, mode.toString())
                .build();

        assertInvocations(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name LIKE 'test_select_s_m_t_comments%'",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, switch (mode) {
                            case NONE, V1, V2 -> 1;
                            case V3 -> 0;
                        })
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, switch (mode) {
                            case NONE, V1, V2 -> 1;
                            case V3 -> 0;
                        })
                        .addCopies(GET_TABLES_WITH_PARAMETER, switch (mode) {
                            case NONE, V1, V2 -> 1;
                            case V3 -> 0;
                        })
                        .addCopies(GET_TABLE, switch (mode) {
                            case NONE, V1, V2 -> tables * 2;
                            case V3 -> 0; // 🎉
                        })
                        .addCopies(STREAM_TABLES, switch (mode) {
                            case NONE, V1, V2 -> 0;
                            case V3 -> 1;
                        })
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(METADATA_JSON, "00004.metadata.json", INPUT_FILE_NEW_STREAM), switch (mode) {
                                case NONE, V1, V2 -> tables;
                                case V3 -> 0; // 🎉
                            })
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), tables)
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), tables)
                            .build();
                });

        assertInvocations(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name = 'test_select_s_m_t_comments" + 0 + "'",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, occurrences(type, 2, 2, 3))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(METADATA_JSON, "00004.metadata.json", INPUT_FILE_NEW_STREAM), 1)
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), 1)
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 1)
                            .build();
                });

        InformationSchemaQueriesAcceleration defaultMode = new ObjectStoreConfig().getInformationSchemaQueriesAcceleration();
        if (mode != defaultMode) {
            assertThat(query(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name LIKE 'test_select_s_m_t_comments%'"))
                    .matches(computeActual("SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name LIKE 'test_select_s_m_t_comments%'"));
        }
    }

    @DataProvider
    public Object[][] metadataTestDataProvider()
    {
        return DataProviders.cartesianProduct(
                tableTypeDataProvider(),
                Stream.of(InformationSchemaQueriesAcceleration.values())
                        .collect(toDataProvider()),
                new Object[][] {
                        {3},
                        {MAX_PREFIXES_COUNT},
                        {MAX_PREFIXES_COUNT + 3}});
    }

    @Test
    public void testInformationSchemaColumnsWithMixedTableTypes()
    {
        assertUpdate("CREATE TABLE test_select_i_s_columns_delta (delta_id VARCHAR, delta_age INT) WITH (type = 'DELTA')");
        assertUpdate("CREATE TABLE test_select_i_s_columns_iceberg (iceberg_id VARCHAR, iceberg_age INT) WITH (type = 'ICEBERG')");
        assertUpdate("CREATE TABLE test_select_i_s_columns_hive (hive_id VARCHAR, hive_age INT) WITH (type = 'HIVE')");

        assertQuery(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = '" + SCHEMA_NAME + "'",
                "VALUES 'delta_id', 'delta_age', 'iceberg_id', 'iceberg_age', 'hive_id', 'hive_age'");

        assertInvocations("TABLE information_schema.columns",
                ImmutableMultiset.builder()
                        .addCopies(GET_ALL_DATABASES, 1)
                        .addCopies(STREAM_TABLES, 1)
                        .build(),
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                        .build());
    }

    @DataProvider
    public Object[][] tableTypeDataProvider()
    {
        return Arrays.stream(TableType.values())
                .collect(toDataProvider());
    }

    private void assertInvocations(@Language("SQL") String query, Multiset<?> expectedMetastoreInvocations, Multiset<FileOperation> expectedFileAccesses)
    {
        assertInvocations(getQueryRunner().getDefaultSession(), query, expectedMetastoreInvocations, expectedFileAccesses);
    }

    private void assertInvocations(Session session, @Language("SQL") String query, Multiset<?> expectedMetastoreInvocations, Multiset<FileOperation> expectedFileAccesses)
    {
        trackingFileSystemFactory.reset();
        CountingAccessHiveMetastoreUtil.assertMetastoreInvocations(metastore, getQueryRunner(), session, query, expectedMetastoreInvocations);
        assertMultisetsEqual(getOperations(), expectedFileAccesses);
    }

    private Multiset<FileOperation> getOperations()
    {
        return trackingFileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), FileOperation.create(
                        entry.getKey().location().path(),
                        entry.getKey().operationType())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private static int occurrences(TableType tableType, int hiveValue, int icebergValue, int deltaValue)
    {
        checkArgument(!(hiveValue == icebergValue && icebergValue == deltaValue), "No need to use Occurrences when hive, iceberg and delta values are same");
        return switch (tableType) {
            case HIVE -> hiveValue;
            case ICEBERG -> icebergValue;
            case DELTA -> deltaValue;
        };
    }

    /**
     * An enum similar to {@link io.trino.plugin.objectstore.TableType} containing only the options tested here.
     */
    enum TableType
    {
        HIVE,
        ICEBERG,
        DELTA,
        // HUDI -- TODO include Hudi when it supports creating tables. Then replace this enum with io.trino.plugin.objectstore.TableType
    }

    private record FileOperation(FileType fileType, String fileId, TrackingFileSystemFactory.OperationType operationType)
    {
        public static final String QUERY_ID_PATTERN = "\\d{8}_\\d{6}_\\d{5}_\\w{5}";
        private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
        private static final Pattern DATA_FILE_PATTERN = Pattern.compile(".*?/(?<partition>key=[^/]*/)?(?<queryId>" + QUERY_ID_PATTERN + ")[-_](?<uuid>" + UUID_PATTERN + ")(\\.orc|\\.parquet)?");

        public static FileOperation create(String path, TrackingFileSystemFactory.OperationType operationType)
        {
            String fileName = path.replaceFirst(".*/", "");

            if (path.contains("/metadata/") && path.endsWith("metadata.json")) {
                return new FileOperation(METADATA_JSON, "%05d.metadata.json".formatted(IcebergUtil.parseVersion(fileName)), operationType);
            }
            if (path.contains("/metadata/") && path.contains("/snap-")) {
                String fileId = fileName.replaceFirst("snap-(?<randomNumber>\\d+)-(?<number>\\d+)-(?<uuid>" + UUID_PATTERN + ").avro", "snap-${number}.avro");
                return new FileOperation(SNAPSHOT, fileId, operationType);
            }
            if (path.contains("/metadata/") && path.endsWith("-m0.avro")) {
                return new FileOperation(MANIFEST, "", operationType);
            }
            if (path.contains("metadata") && path.endsWith(".stats")) {
                return new FileOperation(STATS, "", operationType);
            }
            if (path.matches(".*/_delta_log/_last_checkpoint")) {
                return new FileOperation(LAST_CHECKPOINT, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/\\d+\\.json")) {
                return new FileOperation(TRANSACTION_LOG_JSON, fileName, operationType);
            }
            if (path.matches(".*/_delta_log/_(trino|starburst)_meta/(extended_stats|extendeded_stats).json")) {
                return new FileOperation(TRINO_EXTENDED_STATS_JSON, fileName, operationType);
            }
            if (path.matches(".*/_change_data/.*")) {
                Matcher matcher = DATA_FILE_PATTERN.matcher(path);
                if (matcher.matches()) {
                    return new FileOperation(CDF_DATA, matcher.group("partition"), operationType);
                }
            }

            if (!path.contains("_delta_log") && !path.contains("metadata")) {
                Matcher matcher = DATA_FILE_PATTERN.matcher(path);
                if (matcher.matches()) {
                    return new FileOperation(DATA, firstNonNull(matcher.group("partition"), "no partition"), operationType);
                }
            }

            throw new IllegalArgumentException("File not recognized: " + path);
        }

        public FileOperation
        {
            requireNonNull(fileType, "fileType is null");
            requireNonNull(fileId, "fileId is null");
            requireNonNull(operationType, "operationType is null");
        }
    }

    enum FileType
    {
        // Iceberg
        METADATA_JSON,
        SNAPSHOT,
        MANIFEST,
        STATS,

        // Delta
        LAST_CHECKPOINT,
        TRANSACTION_LOG_JSON,
        TRINO_EXTENDED_STATS_JSON,
        CDF_DATA,

        // Delta, Iceberg
        DATA,
        /**/;
    }
}
