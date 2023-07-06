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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.HdfsNamenodeStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.plugin.hive.aws.athena.PartitionProjectionService;
import io.trino.plugin.hive.fs.FileSystemDirectoryLister;
import io.trino.plugin.hive.fs.TransactionScopeCachingDirectoryListerFactory;
import io.trino.plugin.hive.metastore.HiveCacheTableId;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.UnimplementedHiveMetastore;
import io.trino.plugin.hive.security.SqlStandardAccessControlMetadata;
import io.trino.plugin.hive.util.HiveBlockEncodingSerde;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.version.EmbedVersion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.spi.connector.MetadataProvider.NOOP_METADATA_PROVIDER;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheIds
{
    private ScheduledExecutorService executorService;
    private HiveMetadata metadata;
    private HiveSplitManager splitManager;

    @BeforeClass
    public void setup()
    {
        executorService = newScheduledThreadPool(1);
        HiveConfig config = new HiveConfig();
        HdfsConfiguration hdfsConfiguration = (context, uri) -> newEmptyConfiguration();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication());
        HivePartitionManager hivePartitionManager = new HivePartitionManager(config);
        HiveMetadataFactory metadataFactory = new HiveMetadataFactory(
                LocationAccessControl.ALLOW_ALL,
                new CatalogName("hive"),
                config,
                new HiveMetastoreConfig(),
                HiveMetastoreFactory.ofInstance(new UnimplementedHiveMetastore()),
                new HdfsFileSystemFactory(hdfsEnvironment, HDFS_FILE_SYSTEM_STATS),
                hdfsEnvironment,
                hivePartitionManager,
                newDirectExecutorService(),
                executorService,
                TESTING_TYPE_MANAGER,
                NOOP_METADATA_PROVIDER,
                new HiveLocationService(hdfsEnvironment, config),
                JsonCodec.jsonCodec(PartitionUpdate.class),
                new NodeVersion("test_version"),
                new NoneHiveRedirectionsProvider(),
                ImmutableSet.of(
                        new PartitionsSystemTableProvider(hivePartitionManager, TESTING_TYPE_MANAGER),
                        new PropertiesSystemTableProvider()),
                new DefaultHiveMaterializedViewMetadataFactory(),
                SqlStandardAccessControlMetadata::new,
                new FileSystemDirectoryLister(),
                new TransactionScopeCachingDirectoryListerFactory(config),
                new PartitionProjectionService(config, ImmutableMap.of(), new TestingTypeManager()),
                createJsonCodec(HiveCacheTableId.class),
                createJsonCodec(HiveColumnHandle.class),
                true);
        metadata = (HiveMetadata) metadataFactory.create(ConnectorIdentity.ofUser("user"), true);
        splitManager = new HiveSplitManager(
                config,
                new HiveTransactionManager(metadataFactory),
                hivePartitionManager,
                new MemoryFileSystemFactory(),
                new HdfsNamenodeStats(),
                hdfsEnvironment,
                executorService,
                new EmbedVersion("test"),
                new TestingTypeManager(),
                createJsonCodec(HiveCacheSplitId.class));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
    }

    @Test
    public void testTableId()
    {
        HiveColumnHandle partitionColumn = createBaseColumn("col1", 0, HIVE_INT, INTEGER, PARTITION_KEY, Optional.empty());
        // column list shouldn't be part of table id
        assertThat(metadata.getCacheTableId(createHiveTableHandle(
                "schema",
                "table",
                ImmutableList.of(partitionColumn),
                TupleDomain.all(),
                TupleDomain.all())))
                .isEqualTo(metadata.getCacheTableId(createHiveTableHandle(
                        "schema",
                        "table",
                        ImmutableList.of(),
                        TupleDomain.all(),
                        TupleDomain.all())));

        // enforced predicate shouldn't be part of table id
        assertThat(metadata.getCacheTableId(createHiveTableHandle(
                "schema",
                "table",
                ImmutableList.of(partitionColumn),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, singleValue(INTEGER, 1L))))))
                .isEqualTo(metadata.getCacheTableId(createHiveTableHandle(
                        "schema",
                        "table",
                        ImmutableList.of(partitionColumn),
                        TupleDomain.all(),
                        TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, singleValue(INTEGER, 2L))))));

        // effective predicate should be part of table id
        assertThat(metadata.getCacheTableId(createHiveTableHandle(
                "schema",
                "table",
                ImmutableList.of(),
                TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, singleValue(INTEGER, 1L))),
                TupleDomain.all())))
                .isNotEqualTo(metadata.getCacheTableId(createHiveTableHandle(
                        "schema",
                        "table",
                        ImmutableList.of(),
                        TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, singleValue(INTEGER, 2L))),
                        TupleDomain.all())));
    }

    @Test
    public void testColumnId()
    {
        HiveTableHandle tableHandle = createHiveTableHandle(
                "schema",
                "table",
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all());
        // comment shouldn't be part of column id
        assertThat(metadata.getCacheColumnId(
                tableHandle,
                createBaseColumn(
                        "col",
                        0,
                        HIVE_INT,
                        INTEGER,
                        PARTITION_KEY,
                        Optional.of("comment"))))
                .isEqualTo(metadata.getCacheColumnId(
                        tableHandle,
                        createBaseColumn(
                                "col",
                                0,
                                HIVE_INT,
                                INTEGER,
                                PARTITION_KEY,
                                Optional.of("other comment"))));

        // different column names should change column id
        assertThat(metadata.getCacheColumnId(
                tableHandle,
                createBaseColumn(
                        "col1",
                        0,
                        HIVE_INT,
                        INTEGER,
                        PARTITION_KEY,
                        Optional.empty())))
                .isNotEqualTo(metadata.getCacheColumnId(
                        tableHandle,
                        createBaseColumn(
                                "col2",
                                0,
                                HIVE_INT,
                                INTEGER,
                                PARTITION_KEY,
                                Optional.empty())));
    }

    @Test
    public void testSplitId()
    {
        // table name should be stripped from id
        assertThat(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part", new Properties(), OptionalInt.empty())))
                .isEqualTo(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part", new Properties(), OptionalInt.empty())));

        // different properties order in schema shouldn't make ids different
        Properties schema1 = new Properties();
        schema1.put("key1", "value1");
        schema1.put("key2", "value2");
        Properties schema2 = new Properties();
        schema2.put("key2", "value2");
        schema2.put("key1", "value1");
        assertThat(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part", schema1, OptionalInt.empty())))
                .isEqualTo(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part", schema2, OptionalInt.empty())));

        // different path should make ids different
        assertThat(splitManager.getCacheSplitId(createHiveSplit("path1", 10, "part", new Properties(), OptionalInt.empty())))
                .isNotEqualTo(splitManager.getCacheSplitId(createHiveSplit("path2", 10, "part", new Properties(), OptionalInt.empty())));

        // different length should make ids different
        assertThat(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part", new Properties(), OptionalInt.empty())))
                .isNotEqualTo(splitManager.getCacheSplitId(createHiveSplit("path", 11, "part", new Properties(), OptionalInt.empty())));

        // different partition name should make ids different
        assertThat(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part1", new Properties(), OptionalInt.empty())))
                .isNotEqualTo(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part2", new Properties(), OptionalInt.empty())));

        // different schema should make ids different
        schema1 = new Properties();
        schema1.put("key", "value1");
        schema2 = new Properties();
        schema2.put("key", "value2");
        assertThat(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part", schema1, OptionalInt.empty())))
                .isNotEqualTo(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part", schema2, OptionalInt.empty())));

        // different read bucket number should make ids different
        assertThat(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part", new Properties(), OptionalInt.empty())))
                .isNotEqualTo(splitManager.getCacheSplitId(createHiveSplit("path", 10, "part", new Properties(), OptionalInt.of(1))));
    }

    private static HiveSplit createHiveSplit(
            String path,
            long length,
            String partitionName,
            Properties schema,
            OptionalInt readBucketNumber)
    {
        return new HiveSplit(
                partitionName,
                path,
                0,
                length,
                10,
                12,
                schema,
                ImmutableList.of(),
                ImmutableList.of(),
                readBucketNumber,
                OptionalInt.empty(),
                false,
                new TableToPartitionMapping(Optional.empty(), ImmutableMap.of()),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty(),
                0,
                SplitWeight.standard());
    }

    private static HiveTableHandle createHiveTableHandle(
            String schemaName,
            String tableName,
            List<HiveColumnHandle> partitionColumns,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            TupleDomain<ColumnHandle> enforcedConstraint)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                partitionColumns,
                ImmutableList.of(),
                compactEffectivePredicate,
                enforcedConstraint,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                NO_ACID_TRANSACTION);
    }

    public static <T> JsonCodec<T> createJsonCodec(Class<T> clazz)
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        TypeDeserializer typeDeserializer = new TypeDeserializer(new TestingTypeManager());
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.of(
                        Block.class, new TestingBlockJsonSerde.Deserializer(new HiveBlockEncodingSerde()),
                        Type.class, typeDeserializer));
        objectMapperProvider.setJsonSerializers(ImmutableMap.of(Block.class, new TestingBlockJsonSerde.Serializer(new HiveBlockEncodingSerde())));
        return new JsonCodecFactory(objectMapperProvider).jsonCodec(clazz);
    }
}
