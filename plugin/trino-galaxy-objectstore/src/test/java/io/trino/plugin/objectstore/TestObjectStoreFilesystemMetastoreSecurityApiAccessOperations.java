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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.Session;
import io.trino.filesystem.TrackingFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DataProviders;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_EXISTS;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_CREATE;
import static io.trino.filesystem.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_CREATE_OR_OVERWRITE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_DATABASES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITIONS_BY_NAMES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITION_NAMES_BY_FILTER;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_PARTITION_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.REPLACE_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.STREAM_TABLES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.UPDATE_PARTITION_STATISTICS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.UPDATE_TABLE_STATISTICS;
import static io.trino.plugin.objectstore.TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations.FileType.CDF_DATA;
import static io.trino.plugin.objectstore.TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations.FileType.DATA;
import static io.trino.plugin.objectstore.TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations.FileType.LAST_CHECKPOINT;
import static io.trino.plugin.objectstore.TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations.FileType.MANIFEST;
import static io.trino.plugin.objectstore.TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations.FileType.METADATA_JSON;
import static io.trino.plugin.objectstore.TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations.FileType.SNAPSHOT;
import static io.trino.plugin.objectstore.TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations.FileType.STATS;
import static io.trino.plugin.objectstore.TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations.FileType.TRANSACTION_LOG_JSON;
import static io.trino.plugin.objectstore.TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations.FileType.TRINO_EXTENDED_STATS_JSON;
import static io.trino.plugin.objectstore.TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations.TableType.ICEBERG;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Test filesystem, metastore and portal's security API accesses when accessing data and metadata with ObjectStore connector.
 *
 * @see TestObjectStoreGalaxyMetastoreMetadataQueriesAccessOperations
 */
@Execution(SAME_THREAD) // metastore and filesystem invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestObjectStoreFilesystemMetastoreSecurityApiAccessOperations
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 5;
    private static final String CATALOG_NAME = "objectstore";
    private static final String SCHEMA_NAME = "test_schema";

    private InMemorySpanExporter spanExporter;
    private CountingAccessHiveMetastore metastore;
    private TrackingFileSystemFactory trackingFileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        spanExporter = closeAfterClass(InMemorySpanExporter.create());
        closeAfterClass(() -> spanExporter = null);
        SpanProcessor spanProcessor = SimpleSpanProcessor.create(spanExporter);

        Path schemaDirectory = createTempDirectory(null);
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());

        TestingGalaxyMetastore galaxyMetastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));
        metastore = new CountingAccessHiveMetastore(new GalaxyHiveMetastore(galaxyMetastore.getMetastore(), HDFS_FILE_SYSTEM_FACTORY, schemaDirectory.toUri().toString(), new GalaxyHiveMetastoreConfig().isBatchMetadataFetch()));
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
                        // Enable Delta table creation
                        "DELTA__delta.enable-non-concurrent-writes", "true"));
        properties = Maps.filterEntries(
                properties,
                entry -> !entry.getKey().equals("HIVE__hive.metastore") &&
                        !entry.getKey().equals("ICEBERG__iceberg.catalog.type") &&
                        !entry.getKey().equals("DELTA__hive.metastore") &&
                        !entry.getKey().equals("HUDI__hive.metastore"));

        DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder(CATALOG_NAME, SCHEMA_NAME)
                .setSpanProcessor(spanProcessor)
                .setAccountClient(testingAccountFactory.createAccountClient())
                .addPlugin(new TpchPlugin())
                .addCatalog("tpch", "tpch", true, Map.of())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new TestingObjectStorePlugin(metastore, trackingFileSystemFactory))
                .addCatalog(CATALOG_NAME, "galaxy_objectstore", false, properties)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addCoordinatorProperty("hide-inaccessible-columns", "true") // Galaxy always sets this // TODO set in GalaxyQueryRunner
                .build();
        queryRunner.execute("CREATE SCHEMA %s.%s WITH (location = '%s')".formatted(CATALOG_NAME, SCHEMA_NAME, schemaDirectory.toUri().toString()));
        return queryRunner;
    }

    @AfterEach
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

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testCreateTable(TableType type)
    {
        assertInvocations("CREATE TABLE test_create(id VARCHAR, age INT) WITH (type = '" + type + "')",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_DATABASE)
                        .add(CREATE_TABLE)
                        .addCopies(GET_TABLE, occurrences(type, 1, 1, 2))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", OUTPUT_FILE_CREATE))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testCreateTableAsSelect(TableType type)
    {
        // Prime the RelationTypeCache to make the test deterministic
        assertUpdate("CREATE TABLE test_ctas(a bigint) WITH (type = '" + type + "')");
        assertUpdate("DROP TABLE test_ctas");

        assertInvocations("CREATE TABLE test_ctas WITH (type = '" + type + "') AS SELECT 1 AS age",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(CREATE_TABLE)
                        .addCopies(GET_TABLE, occurrences(type, 1, 4, 1))
                        .add(GET_DATABASE)
                        .addCopies(REPLACE_TABLE, occurrences(type, 0, 1, 0))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of(
                            new FileOperation(DATA, "no partition", OUTPUT_FILE_CREATE));
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(STATS, "", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(MANIFEST, "", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_EXISTS))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", OUTPUT_FILE_CREATE))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testCreateTableAsSelectFromDifferentCatalog(TableType type)
    {
        testCreateTableAsSelectFromDifferentCatalog(type, "SELECT * FROM tpch.tiny.nation", false);
        testCreateTableAsSelectFromDifferentCatalog(type, "TABLE tpch.tiny.nation", true);
    }

    private void testCreateTableAsSelectFromDifferentCatalog(TableType type, @Language("SQL") String sourceForm, boolean tableForm)
    {
        // Need random name because this test creates tables twice
        String tableName = "test_ctas_different_catalog" + randomNameSuffix();

        // Prime the RelationTypeCache to make the test deterministic
        assertUpdate("CREATE TABLE " + tableName + "(a bigint) WITH (type = '" + type + "')");
        assertUpdate("DROP TABLE " + tableName);

        assertInvocations(
                getSession(),
                "CREATE TABLE " + tableName + " WITH (type = '" + type + "') AS " + sourceForm,
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_DATABASE)
                        .addCopies(GET_TABLE, occurrences(type, 1, 4, 1))
                        .add(CREATE_TABLE)
                        .addCopies(REPLACE_TABLE, occurrences(type, 0, 1, 0))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of(
                            new FileOperation(DATA, "no partition", OUTPUT_FILE_CREATE));
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(METADATA_JSON, "00000.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(STATS, "", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(MANIFEST, "", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(DATA, "no partition", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_EXISTS))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", OUTPUT_FILE_CREATE))
                            .build();
                },
                ImmutableList.<TracesAssertion>builder()
                        .add(TracesAssertion.builder()
                                .filterByAttribute("airlift.http.client_name", "galaxy-access-control")
                                .formattingName()
                                .formattingUriAttribute("url.full", uri -> uri.getPath()
                                        // TODO differentiate source and target catalogs
                                        .replaceAll("(/[cr])-\\d+(/|$)", "$1-xxx$2")
                                        .replace(tableName, "test_ctas_different_catalog"))
                                .setExpected(ImmutableMultiset.<String>builder()
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/role")
                                        .addCopies("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/schema/c-xxx/tiny/tableVisibility", tableForm ? 0 : 1) // TODO due to Trino bug, hide-inaccessible-columns does not apply to "AS TABLE t"
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/schema/c-xxx/test_schema/privileges/r-xxx")
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/tiny/nation/privileges/r-xxx")
                                        .add("galaxy-access-control POST /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_ctas_different_catalog/:create")
                                        .build())
                                .build())
                        .build());
    }

    @Test
    public void testCreateReplaceViewFromDifferentCatalog()
    {
        // Need random name because this test creates tables twice
        String viewName = "test_create_view_different_catalog";

        // Prime the RelationTypeCache to make the test deterministic
        assertUpdate("CREATE TABLE " + viewName + "(a bigint)");
        assertUpdate("DROP TABLE " + viewName);

        assertInvocations(
                getSession(),
                "CREATE OR REPLACE VIEW " + viewName + " SECURITY INVOKER AS SELECT * FROM tpch.tiny.nation",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .addCopies(GET_TABLE, 2)
                        .add(CREATE_TABLE)
                        .build(),
                ImmutableMultiset.of(),
                ImmutableList.<TracesAssertion>builder()
                        .add(TracesAssertion.builder()
                                .filterByAttribute("airlift.http.client_name", "galaxy-access-control")
                                .formattingName()
                                .formattingUriAttribute("url.full", uri -> uri.getPath()
                                        // TODO differentiate source and target catalogs
                                        .replaceAll("(/[cr])-\\d+(/|$)", "$1-xxx$2"))
                                .setExpected(ImmutableMultiset.<String>builder()
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/role")
                                        .add("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/schema/c-xxx/tiny/tableVisibility")
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/schema/c-xxx/test_schema/privileges/r-xxx")
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/tiny/nation/privileges/r-xxx")
                                        .add("galaxy-access-control POST /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_create_view_different_catalog/:create")
                                        .build())
                                .build())
                        .build());
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testSelectFromEmpty(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_from(id VARCHAR, age INT) WITH (type = '" + type + "')");

        assertInvocations("SELECT * FROM test_select_from",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
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
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testSelectPartitionedTable(TableType type)
    {
        String partitionProperty = type == ICEBERG ? "partitioning" : "partitioned_by";
        assertUpdate("CREATE TABLE test_select_partition WITH (" + partitionProperty + " = ARRAY['part'], type = '" + type + "') AS SELECT 1 AS data, 10 AS part", 1);

        assertInvocations("SELECT * FROM test_select_partition",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
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
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                });

        assertUpdate("INSERT INTO test_select_partition SELECT 2 AS data, 20 AS part", 1);
        assertInvocations("SELECT * FROM test_select_partition",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
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
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });

        // Specify a specific partition
        assertInvocations("SELECT * FROM test_select_partition WHERE part = 10",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
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
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testSelectWithFilter(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_from_where WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertInvocations("SELECT * FROM test_select_from_where WHERE age = 2",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
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
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testSelectFromView(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_view_table(id VARCHAR, age INT) WITH (type = '" + type + "')");
        assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

        assertInvocations("SELECT * FROM test_select_view_view",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .addCopies(GET_TABLE, 2)
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
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testSelectFromViewWithFilter(TableType type)
    {
        assertUpdate("CREATE TABLE test_select_view_where_table WITH (type = '" + type + "') AS SELECT 2 AS age", 1);
        assertUpdate("CREATE VIEW test_select_view_where_view AS SELECT age FROM test_select_view_where_table");

        assertInvocations("SELECT * FROM test_select_view_where_view WHERE age = 2",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .addCopies(GET_TABLE, 2)
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
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testJoin(TableType type)
    {
        assertUpdate("CREATE TABLE test_join_t1 WITH (type = '" + type + "') AS SELECT 2 AS age, 'id1' AS id", 1);
        assertUpdate("CREATE TABLE test_join_t2 WITH (type = '" + type + "') AS SELECT 'name1' AS name, 'id1' AS id", 1);

        assertInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .addCopies(GET_TABLE, 2)
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
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM), 2)
                            .addCopies(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testSelfJoin(TableType type)
    {
        assertUpdate("CREATE TABLE test_self_join_table WITH (type = '" + type + "') AS SELECT 2 AS age, 0 parent, 3 AS id", 1);

        assertInvocations("SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
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
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testExplainSelect(TableType type)
    {
        assertUpdate("CREATE TABLE test_explain WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertInvocations("EXPLAIN SELECT * FROM test_explain",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
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
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testShowStatsForTable(TableType type)
    {
        assertUpdate("CREATE TABLE test_show_stats WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertInvocations("SHOW STATS FOR test_show_stats",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
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
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testShowStatsForTableWithFilter(TableType type)
    {
        assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 AS age", 1);

        assertInvocations("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter where age >= 2)",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
                        .add(GET_TABLE_STATISTICS)
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of();
                    case ICEBERG -> ImmutableMultiset.of();
                    case DELTA -> ImmutableMultiset.of();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testAnalyze(TableType type)
    {
        assertUpdate("CREATE TABLE test_analyze WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        assertInvocations("ANALYZE test_analyze",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .addCopies(GET_TABLE, occurrences(type, 1, 3, 1))
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
                            .add(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(STATS, "", OUTPUT_FILE_CREATE))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_EXISTS))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testAnalyzePartitionedTable(TableType type)
    {
        String partitionProperty = type == ICEBERG ? "partitioning" : "partitioned_by";
        assertUpdate("CREATE TABLE test_analyze_partition WITH (" + partitionProperty + " = ARRAY['part'], type = '" + type + "') AS SELECT 1 AS data, 10 AS part", 1);

        assertInvocations("ANALYZE test_analyze_partition",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .addCopies(GET_TABLE, occurrences(type, 1, 3, 1))
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
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(SNAPSHOT, "snap-1.avro", INPUT_FILE_GET_LENGTH))
                            .add(new FileOperation(DATA, "no partition", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(STATS, "", OUTPUT_FILE_CREATE))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_EXISTS))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .build();
                });

        assertUpdate("INSERT INTO test_analyze_partition SELECT 2 AS data, 20 AS part", 1);

        assertInvocations("ANALYZE test_analyze_partition",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .addCopies(GET_TABLE, occurrences(type, 1, 3, 1))
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
                            .add(new FileOperation(STATS, "", OUTPUT_FILE_CREATE))
                            .addCopies(new FileOperation(MANIFEST, "", INPUT_FILE_NEW_STREAM), 2)
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extendeded_stats.json", INPUT_FILE_EXISTS))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", OUTPUT_FILE_CREATE_OR_OVERWRITE))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testDropStats(TableType type)
    {
        assertUpdate("CREATE TABLE drop_stats WITH (type = '" + type + "') AS SELECT 2 AS age", 1);

        String dropStats = switch (type) {
            case HIVE -> "CALL system.drop_stats('test_schema', 'drop_stats')";
            case ICEBERG -> "ALTER TABLE drop_stats EXECUTE drop_extended_stats";
            case DELTA -> "CALL system.drop_extended_stats('test_schema', 'drop_stats')";
        };
        assertInvocations(dropStats,
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .addCopies(GET_TABLE, occurrences(type, 1, 3, 1))
                        .addCopies(UPDATE_TABLE_STATISTICS, occurrences(type, 1, 0, 0))
                        .addCopies(REPLACE_TABLE, occurrences(type, 0, 1, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_CREATE))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_EXISTS))
                            .build();
                });
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
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
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .addCopies(GET_TABLE, occurrences(type, 1, 3, 1))
                        .addCopies(GET_PARTITION_NAMES_BY_FILTER, occurrences(type, 1, 0, 0))
                        .addCopies(UPDATE_PARTITION_STATISTICS, occurrences(type, 1, 0, 0))
                        .addCopies(REPLACE_TABLE, occurrences(type, 0, 1, 0))
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.<FileOperation>builder()
                            .build();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00002.metadata.json", OUTPUT_FILE_CREATE))
                            .add(new FileOperation(METADATA_JSON, "00001.metadata.json", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_EXISTS))
                            .build();
                });

        assertUpdate("INSERT INTO drop_stats_partition SELECT 2 AS data, 20 AS part", 1);

        assertInvocations(dropStats,
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .addCopies(GET_TABLE, occurrences(type, 1, 2, 1))
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
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRINO_EXTENDED_STATS_JSON, "extended_stats.json", INPUT_FILE_EXISTS))
                            .build();
                });
    }

    @ParameterizedTest
    @MethodSource("metadataTestDataProvider")
    public void testInformationSchemaColumns(TableType type, int tableBatches)
    {
        for (int i = 0; i < tableBatches; i++) {
            assertUpdate("CREATE TABLE test_select_i_s_columns" + i + "(id VARCHAR, age INT) WITH (type = '" + type + "')");
            // Produce multiple snapshots and metadata files
            assertUpdate("INSERT INTO test_select_i_s_columns" + i + " VALUES ('abc', 11)", 1);
            assertUpdate("INSERT INTO test_select_i_s_columns" + i + " VALUES ('xyz', 12)", 1);

            assertUpdate("CREATE TABLE test_other_select_i_s_columns" + i + "(id varchar, age integer) WITH (type = '" + type + "')");
        }

        assertUpdate("CREATE TABLE test_yet_another_other_select_i_s_columns(id varchar, age integer) WITH (type = '" + type + "')"); // won't match the filter

        int allTables = tableBatches * 2 + 1;

        Session session = getSession();

        Multiset<FileOperation> bulkRetrievalFileOperations = switch (type) {
            case HIVE, ICEBERG -> ImmutableMultiset.of();
            case DELTA -> ImmutableMultiset.<FileOperation>builder()
                    .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), allTables)
                    .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), allTables)
                    .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), allTables)
                    .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), tableBatches)
                    .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), tableBatches)
                    .build();
        };

        // Bulk retrieval
        assertInvocations(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'test_select_i_s_columns%'",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(STREAM_TABLES)
                        .build(),
                bulkRetrievalFileOperations,
                ImmutableList.<TracesAssertion>builder()
                        .add(TracesAssertion.builder()
                                .filterByAttribute("airlift.http.client_name", "galaxy-access-control")
                                .formattingName()
                                .formattingUriAttribute("url.full", uri -> uri.getPath()
                                        .replaceAll("(/[cr])-\\d+(/|$)", "$1-xxx$2")
                                        .replaceAll("/(test_select_i_s_columns|test_other_select_i_s_columns)\\d+/", "/$1__/"))
                                .setExpected(ImmutableMultiset.<String>builder()
                                        // TODO (https://github.com/starburstdata/stargate/issues/12879) if information_schema.columns privileges are no longer asked for,
                                        //  remove hot-sharing for them from GalaxyPermissionsCache
                                        .add("galaxy-access-control POST /api/v1/galaxy/security/trino/catalogVisibility")
                                        .addCopies("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/schema/c-xxx/test_schema/tableVisibility", tableBatches == 3 ? 1 : 0)
                                        .addCopies("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/catalog/c-xxx/tableVisibility", tableBatches == 3 ? 0 : 1)
                                        .addCopies("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_select_i_s_columns__/privileges/r-xxx", tableBatches)
                                        // TODO AccessControl is consulted even for tables filtered out by the query LIKE predicate (test_other_select_i_s_columns...)
                                        .addCopies("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_other_select_i_s_columns__/privileges/r-xxx", tableBatches)
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_yet_another_other_select_i_s_columns/privileges/r-xxx")
                                        .build())
                                .build())
                        .build());

        // Bulk retrieval specific columns
        assertInvocations(session, "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'test_select_i_s_columns%'",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(STREAM_TABLES)
                        .build(),
                bulkRetrievalFileOperations,
                ImmutableList.<TracesAssertion>builder()
                        .add(TracesAssertion.builder()
                                .filterByAttribute("airlift.http.client_name", "galaxy-access-control")
                                .formattingName()
                                .formattingUriAttribute("url.full", uri -> uri.getPath()
                                        .replaceAll("(/[cr])-\\d+(/|$)", "$1-xxx$2")
                                        .replaceAll("/(test_select_i_s_columns|test_other_select_i_s_columns)\\d+/", "/$1__/"))
                                .setExpected(ImmutableMultiset.<String>builder()
                                        // TODO (https://github.com/starburstdata/stargate/issues/12879) if information_schema.columns privileges are no longer asked for,
                                        //  remove hot-sharing for them from GalaxyPermissionsCache
                                        .addCopies("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/schema/c-xxx/test_schema/tableVisibility", tableBatches == 3 ? 1 : 0)
                                        .addCopies("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/catalog/c-xxx/tableVisibility", tableBatches == 3 ? 0 : 1)
                                        .addCopies("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_select_i_s_columns__/privileges/r-xxx", tableBatches)
                                        // TODO AccessControl is consulted even for tables filtered out by the query LIKE predicate (test_other_select_i_s_columns...)
                                        .addCopies("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_other_select_i_s_columns__/privileges/r-xxx", tableBatches)
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_yet_another_other_select_i_s_columns/privileges/r-xxx")
                                        .build())
                                .build())
                        .build());

        // Bulk retrieval without filters. Including information_schema schema involves InformationSchemaMetadata and may result e.g. in additional access control calls
        assertInvocations(session, "TABLE information_schema.columns",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_ALL_DATABASES)
                        .add(STREAM_TABLES)
                        .build(),
                bulkRetrievalFileOperations,
                ImmutableList.<TracesAssertion>builder()
                        .add(TracesAssertion.builder()
                                .filterByAttribute("airlift.http.client_name", "galaxy-access-control")
                                .formattingName()
                                .formattingUriAttribute("url.full", uri -> uri.getPath()
                                        .replaceAll("(/[cr])-\\d+(/|$)", "$1-xxx$2")
                                        .replaceAll("/((test_select_i_s_columns|test_other_select_i_s_columns)\\d+|test_yet_another_other_select_i_s_columns)/", "/table-xxx/"))
                                .setExpected(ImmutableMultiset.<String>builder()
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/role")
                                        .add("galaxy-access-control POST /api/v1/galaxy/security/trino/catalogVisibility")
                                        .add("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/catalog/c-xxx/tableVisibility")
                                        .addCopies("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/schema/c-xxx/test_schema/tableVisibility", tableBatches == 3 ? 1 : 0)
                                        .addCopies("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/table-xxx/privileges/r-xxx", allTables)
                                        .build())
                                .build())
                        .build());

        Multiset<FileOperation> pointedLookupFileOperations = switch (type) {
            case HIVE -> ImmutableMultiset.of();
            case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                    .add(new FileOperation(METADATA_JSON, "00004.metadata.json", INPUT_FILE_NEW_STREAM))
                    .build();
            case DELTA -> ImmutableMultiset.<FileOperation>builder()
                    .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                    .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                    .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                    .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM))
                    .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM))
                    .build();
        };

        // Pointed lookup
        assertInvocations(session, "SELECT * FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'test_select_i_s_columns0'",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
                        .build(),
                pointedLookupFileOperations,
                ImmutableList.<TracesAssertion>builder()
                        .add(TracesAssertion.builder()
                                .filterByAttribute("airlift.http.client_name", "galaxy-access-control")
                                .formattingName()
                                .formattingUriAttribute("url.full", uri -> uri.getPath()
                                        .replaceAll("(/[cr])-\\d+(/|$)", "$1-xxx$2"))
                                .setExpected(ImmutableMultiset.<String>builder()
                                        .add("galaxy-access-control POST /api/v1/galaxy/security/trino/catalogVisibility")
                                        .add("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/schema/c-xxx/test_schema/tableVisibility")
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_select_i_s_columns0/privileges/r-xxx")
                                        .build())
                                .build())
                        .build());

        // Pointed lookup specific columns
        assertInvocations(session, "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = 'test_select_i_s_columns0'",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
                        .build(),
                pointedLookupFileOperations,
                ImmutableList.<TracesAssertion>builder()
                        .add(TracesAssertion.builder()
                                .filterByAttribute("airlift.http.client_name", "galaxy-access-control")
                                .formattingName()
                                .formattingUriAttribute("url.full", uri -> uri.getPath()
                                        .replaceAll("(/[cr])-\\d+(/|$)", "$1-xxx$2"))
                                .setExpected(ImmutableMultiset.<String>builder()
                                        .add("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/schema/c-xxx/test_schema/tableVisibility")
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_select_i_s_columns0/privileges/r-xxx")
                                        .build())
                                .build())
                        .build());

        // Pointed lookup via DESCRIBE (which does some additional things before delegating to information_schema.columns)
        assertInvocations(session, "DESCRIBE test_select_i_s_columns0",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .build(),
                pointedLookupFileOperations,
                ImmutableList.<TracesAssertion>builder()
                        .add(TracesAssertion.builder()
                                .filterByAttribute("airlift.http.client_name", "galaxy-access-control")
                                .formattingName()
                                .formattingUriAttribute("url.full", uri -> uri.getPath()
                                        .replaceAll("(/[cr])-\\d+(/|$)", "$1-xxx$2"))
                                .setExpected(ImmutableMultiset.<String>builder()
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/role")
                                        .add("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/schema/c-xxx/test_schema/tableVisibility")
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_select_i_s_columns0/privileges/r-xxx")
                                        .build())
                                .build())
                        .build());
    }

    @ParameterizedTest
    @MethodSource("metadataTestDataProvider")
    public void testSystemMetadataTableComments(TableType type, int tableBatches)
    {
        for (int i = 0; i < tableBatches; i++) {
            assertUpdate("CREATE TABLE test_select_s_m_t_comments" + i + "(id VARCHAR, age INT) WITH (type = '" + type + "')");
            // Produce multiple snapshots and metadata files
            assertUpdate("INSERT INTO test_select_s_m_t_comments" + i + " VALUES ('abc', 11)", 1);
            assertUpdate("INSERT INTO test_select_s_m_t_comments" + i + " VALUES ('xyz', 12)", 1);

            assertUpdate("CREATE TABLE test_other_select_s_m_t_comments" + i + "(id varchar, age integer) WITH (type = '" + type + "')");
        }

        int allTables = tableBatches * 2;

        Session session = getSession();

        // Bulk retrieval
        // TODO add assertions for galaxy-access-control. When doing so, test separately `SELECT *` and `SELECT <explicit-columns` cases
        assertInvocations(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name LIKE 'test_select_s_m_t_comments%'",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(STREAM_TABLES)
                        .build(),
                switch (type) {
                    case HIVE, ICEBERG -> ImmutableMultiset.of();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .addCopies(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM), allTables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM), allTables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM), allTables)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM), tableBatches)
                            .addCopies(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM), tableBatches)
                            .build();
                });

        // Pointed lookup
        // TODO add assertions for galaxy-access-control. When doing so, test separately `SELECT *` and `SELECT <explicit-columns` cases
        assertInvocations(session, "SELECT * FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA AND table_name = 'test_select_s_m_t_comments" + 0 + "'",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_TABLE)
                        .build(),
                switch (type) {
                    case HIVE -> ImmutableMultiset.of();
                    case ICEBERG -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(METADATA_JSON, "00004.metadata.json", INPUT_FILE_NEW_STREAM))
                            .build();
                    case DELTA -> ImmutableMultiset.<FileOperation>builder()
                            .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000002.json", INPUT_FILE_NEW_STREAM))
                            .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000003.json", INPUT_FILE_NEW_STREAM))
                            .build();
                });
    }

    public Object[][] metadataTestDataProvider()
    {
        return DataProviders.cartesianProduct(
                Stream.of(TableType.values())
                        .collect(toDataProvider()),
                new Object[][] {
                        {3},
                        {MAX_PREFIXES_COUNT},
                        {MAX_PREFIXES_COUNT + 3},
                });
    }

    @Test
    public void testInformationSchemaColumnsWithMixedTableTypes()
    {
        assertUpdate("CREATE TABLE test_select_i_s_columns_delta (delta_id VARCHAR, delta_age INT) WITH (type = 'DELTA')");
        assertUpdate("CREATE TABLE test_select_i_s_columns_iceberg (iceberg_id VARCHAR, iceberg_age INT) WITH (type = 'ICEBERG')");
        assertUpdate("CREATE TABLE test_select_i_s_columns_hive (hive_id VARCHAR, hive_age INT) WITH (type = 'HIVE')");

        Session session = getSession();

        assertQuery(
                session,
                "SELECT column_name FROM information_schema.columns WHERE table_schema = '" + SCHEMA_NAME + "'",
                "VALUES 'delta_id', 'delta_age', 'iceberg_id', 'iceberg_age', 'hive_id', 'hive_age'");

        assertInvocations(session, "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns",
                ImmutableMultiset.<CountingAccessHiveMetastore.Method>builder()
                        .add(GET_ALL_DATABASES)
                        .add(STREAM_TABLES)
                        .build(),
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(LAST_CHECKPOINT, "_last_checkpoint", INPUT_FILE_NEW_STREAM))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000000.json", INPUT_FILE_NEW_STREAM))
                        .add(new FileOperation(TRANSACTION_LOG_JSON, "00000000000000000001.json", INPUT_FILE_NEW_STREAM))
                        .build(),
                ImmutableList.<TracesAssertion>builder()
                        .add(TracesAssertion.builder()
                                .filterByAttribute("airlift.http.client_name", "galaxy-access-control")
                                .formattingName()
                                .formattingUriAttribute("url.full", uri -> uri.getPath()
                                        .replaceAll("(/[cr])-\\d+(/|$)", "$1-xxx$2"))
                                .setExpected(ImmutableMultiset.<String>builder()
                                        .add("galaxy-access-control POST /api/v1/galaxy/security/trino/catalogVisibility")
                                        .add("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/catalog/c-xxx/tableVisibility")
                                        .addCopies("galaxy-access-control PUT /api/v1/galaxy/security/trino/entity/schema/c-xxx/test_schema/tableVisibility", 2)
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_select_i_s_columns_iceberg/privileges/r-xxx")
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_select_i_s_columns_delta/privileges/r-xxx")
                                        .add("galaxy-access-control GET /api/v1/galaxy/security/trino/entity/table/c-xxx/test_schema/test_select_i_s_columns_hive/privileges/r-xxx")
                                        .build())
                                .build())
                        .build());
    }

    private void assertInvocations(@Language("SQL") String query, Multiset<CountingAccessHiveMetastore.Method> expectedMetastoreInvocations, Multiset<FileOperation> expectedFileAccesses)
    {
        assertInvocations(getQueryRunner().getDefaultSession(), query, expectedMetastoreInvocations, expectedFileAccesses);
    }

    private void assertInvocations(Session session, @Language("SQL") String query, Multiset<CountingAccessHiveMetastore.Method> expectedMetastoreInvocations, Multiset<FileOperation> expectedFileAccesses)
    {
        // Delta depends on metadata and active files caches being present, so tuning them down could have us testing too pessimistic cases. Flush before counting instead.
        assertUpdate("CALL system.flush_metadata_cache()");

        trackingFileSystemFactory.reset();
        CountingAccessHiveMetastoreUtil.assertMetastoreInvocations(metastore, getQueryRunner(), session, query, expectedMetastoreInvocations);
        assertMultisetsEqual(getOperations(), expectedFileAccesses);
    }

    private void assertInvocations(
            Session session,
            @Language("SQL") String query,
            Multiset<CountingAccessHiveMetastore.Method> expectedMetastoreInvocations,
            Multiset<FileOperation> expectedFileAccesses,
            List<TracesAssertion> expectedTraces)
    {
        // Delta depends on metadata and active files caches being present, so tuning them down could have us testing too pessimistic cases. Flush before counting instead.
        assertUpdate("CALL system.flush_metadata_cache()");

        spanExporter.reset();
        trackingFileSystemFactory.reset();
        CountingAccessHiveMetastoreUtil.assertMetastoreInvocations(metastore, getQueryRunner(), session, query, expectedMetastoreInvocations);
        Multiset<FileOperation> fileOperations = getOperations();
        List<SpanData> finishedSpans = spanExporter.getFinishedSpanItems();
        assertMultisetsEqual(fileOperations, expectedFileAccesses);
        expectedTraces.forEach(assertion -> assertion.verify(finishedSpans));
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
    }

    private static class TracesAssertion
    {
        private final Predicate<SpanData> filter;
        private final Function<SpanData, String> formatter;
        private final Multiset<String> expected;

        public TracesAssertion(Predicate<SpanData> filter, Function<SpanData, String> formatter, Multiset<String> expected)
        {
            this.filter = requireNonNull(filter, "filter is null");
            this.formatter = requireNonNull(formatter, "formatter is null");
            this.expected = requireNonNull(expected, "expected is null");
        }

        public void verify(List<SpanData> spans)
        {
            assertMultisetsEqual(
                    spans.stream()
                            .filter(filter)
                            .map(formatter)
                            .collect(toImmutableMultiset()),
                    expected);
        }

        public static Builder builder()
        {
            return new Builder();
        }

        private static class Builder
        {
            private Predicate<SpanData> filter = ignore -> true;
            private ImmutableList.Builder<Function<SpanData, String>> formatters = ImmutableList.builder();
            private Multiset<String> expected;

            @CanIgnoreReturnValue
            public Builder filterByAttribute(String key, Object value)
            {
                return filter(spanData -> value.equals(spanData.getAttributes().get(AttributeKey.stringKey(key))));
            }

            private Builder filter(Predicate<SpanData> additionalFilter)
            {
                filter = filter.and(additionalFilter);
                return this;
            }

            @CanIgnoreReturnValue
            public Builder formattingName()
            {
                formatters.add(SpanData::getName);
                return this;
            }

            @CanIgnoreReturnValue
            public Builder formattingAttribute(String key)
            {
                return formattingAttribute(key, Function.identity());
            }

            @CanIgnoreReturnValue
            public Builder formattingUriAttribute(String key, Function<URI, String> valueProcessor)
            {
                return formattingAttribute(key, (String uri) -> valueProcessor.apply(URI.create(uri)));
            }

            @CanIgnoreReturnValue
            public <T> Builder formattingAttribute(String key, Function<T, String> valueProcessor)
            {
                AttributeKey<String> attributeKey = AttributeKey.stringKey(key);
                formatters.add(spanData -> {
                    Object value = spanData.getAttributes().get(attributeKey);
                    if (value == null) {
                        return "null";
                    }
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    Object processed = ((Function) valueProcessor).apply(value);
                    return String.valueOf(processed);
                });
                return this;
            }

            @CanIgnoreReturnValue
            public Builder setExpected(Multiset<String> expected)
            {
                this.expected = requireNonNull(expected, "expected is null");
                return this;
            }

            public TracesAssertion build()
            {
                List<Function<SpanData, String>> formatters = this.formatters.build();
                return new TracesAssertion(
                        filter,
                        spanData -> formatters.stream()
                                .map(format -> format.apply(spanData))
                                .collect(Collectors.joining(" ")),
                        expected);
            }
        }
    }
}
