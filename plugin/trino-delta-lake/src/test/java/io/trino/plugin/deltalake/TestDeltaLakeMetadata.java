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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.trace.Tracer;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.filesystem.cache.NoneCachingHostAddressProvider;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreModule;
import io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorContext;
import io.trino.tests.BogusType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.EXTENDED_STATISTICS_COLLECT_ON_WRITE;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.CHANGE_DATA_FEED_ENABLED_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.COLUMN_MAPPING_MODE_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.NONE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDeltaLakeMetadata
        extends AbstractTestQueryFramework
{
    private static final String DATABASE_NAME = "mock_database";

    private static final ColumnMetadata BIGINT_COLUMN_1 = new ColumnMetadata("bigint_column1", BIGINT);
    private static final ColumnMetadata BIGINT_COLUMN_2 = new ColumnMetadata("bigint_column2", BIGINT);
    private static final ColumnMetadata TIMESTAMP_COLUMN = new ColumnMetadata("timestamp_column", TIMESTAMP_MILLIS);
    private static final ColumnMetadata MISSING_COLUMN = new ColumnMetadata("missing_column", BIGINT);

    private static final RowType BOGUS_ROW_FIELD = RowType.from(ImmutableList.of(
            new RowType.Field(Optional.of("test_field"), BogusType.BOGUS)));
    private static final RowType NESTED_ROW_FIELD = RowType.from(ImmutableList.of(
            new RowType.Field(Optional.of("child1"), INTEGER),
            new RowType.Field(Optional.of("child2"), INTEGER)));

    private static final DeltaLakeColumnHandle BOOLEAN_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("boolean_column_name", BooleanType.BOOLEAN, OptionalInt.empty(), "boolean_column_name", BooleanType.BOOLEAN, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle DOUBLE_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("double_column_name", DoubleType.DOUBLE, OptionalInt.empty(), "double_column_name", DoubleType.DOUBLE, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle BOGUS_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("bogus_column_name", BogusType.BOGUS, OptionalInt.empty(), "bogus_column_name", BogusType.BOGUS, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle VARCHAR_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("varchar_column_name", VarcharType.VARCHAR, OptionalInt.empty(), "varchar_column_name", VarcharType.VARCHAR, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle DATE_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("date_column_name", DateType.DATE, OptionalInt.empty(), "date_column_name", DateType.DATE, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle NESTED_COLUMN_HANDLE =
            new DeltaLakeColumnHandle("nested_column_name", NESTED_ROW_FIELD, OptionalInt.empty(), "nested_column_name", NESTED_ROW_FIELD, REGULAR, Optional.empty());
    private static final DeltaLakeColumnHandle EXPECTED_NESTED_COLUMN_HANDLE =
            new DeltaLakeColumnHandle(
                    "nested_column_name",
                    NESTED_ROW_FIELD,
                    OptionalInt.empty(),
                    "nested_column_name",
                    NESTED_ROW_FIELD,
                    REGULAR,
                    Optional.of(new DeltaLakeColumnProjectionInfo(INTEGER, ImmutableList.of(1), ImmutableList.of("child2"))));

    private static final Map<String, ColumnHandle> SYNTHETIC_COLUMN_ASSIGNMENTS = ImmutableMap.of(
            "test_synthetic_column_name_1", BOGUS_COLUMN_HANDLE,
            "test_synthetic_column_name_2", VARCHAR_COLUMN_HANDLE);
    private static final Map<String, ColumnHandle> NESTED_COLUMN_ASSIGNMENTS = ImmutableMap.of("nested_column_name", NESTED_COLUMN_HANDLE);
    private static final Map<String, ColumnHandle> EXPECTED_NESTED_COLUMN_ASSIGNMENTS = ImmutableMap.of("nested_column_name#child2", EXPECTED_NESTED_COLUMN_HANDLE);

    private static final ConnectorExpression DOUBLE_PROJECTION = new Variable("double_projection", DoubleType.DOUBLE);
    private static final ConnectorExpression BOOLEAN_PROJECTION = new Variable("boolean_projection", BooleanType.BOOLEAN);
    private static final ConnectorExpression DEREFERENCE_PROJECTION = new FieldDereference(
            BOGUS_ROW_FIELD,
            new Constant(1, BOGUS_ROW_FIELD),
            0);
    private static final ConnectorExpression NESTED_DEREFERENCE_PROJECTION = new FieldDereference(
            INTEGER,
            new Variable("nested_column_name", NESTED_ROW_FIELD),
            1);
    private static final ConnectorExpression EXPECTED_NESTED_DEREFERENCE_PROJECTION = new Variable(
            "nested_column_name#child2",
            INTEGER);

    private static final List<ConnectorExpression> SIMPLE_COLUMN_PROJECTIONS =
            ImmutableList.of(DOUBLE_PROJECTION, BOOLEAN_PROJECTION);
    private static final List<ConnectorExpression> DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(DOUBLE_PROJECTION, DEREFERENCE_PROJECTION, BOOLEAN_PROJECTION);
    private static final List<ConnectorExpression> NESTED_DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(NESTED_DEREFERENCE_PROJECTION);
    private static final List<ConnectorExpression> EXPECTED_NESTED_DEREFERENCE_COLUMN_PROJECTIONS =
            ImmutableList.of(EXPECTED_NESTED_DEREFERENCE_PROJECTION);

    private static final Set<DeltaLakeColumnHandle> PREDICATE_COLUMNS =
            ImmutableSet.of(BOOLEAN_COLUMN_HANDLE, DOUBLE_COLUMN_HANDLE);

    private File temporaryCatalogDirectory;
    private DeltaLakeMetadataFactory deltaLakeMetadataFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(DELTA_CATALOG, ImmutableMap.of(), ImmutableMap.of());
    }

    @BeforeAll
    public void setUp()
            throws IOException
    {
        temporaryCatalogDirectory = createTempDirectory("HiveCatalog").toFile();
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", temporaryCatalogDirectory.getPath())
                .buildOrThrow();

        Bootstrap app = new Bootstrap(
                // connector dependencies
                new JsonModule(),
                binder -> {
                    ConnectorContext context = new TestingConnectorContext();
                    binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                    binder.bind(CatalogName.class).toInstance(new CatalogName("test"));
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                    binder.bind(Tracer.class).toInstance(context.getTracer());
                },
                // connector modules
                new DeltaLakeMetastoreModule(),
                new DeltaLakeModule(),
                // test setup
                binder -> {
                    binder.bind(HdfsEnvironment.class).toInstance(HDFS_ENVIRONMENT);
                    binder.bind(TrinoHdfsFileSystemStats.class).toInstance(HDFS_FILE_SYSTEM_STATS);
                    binder.bind(TrinoFileSystemFactory.class).to(HdfsFileSystemFactory.class).in(Scopes.SINGLETON);
                    binder.bind(CachingHostAddressProvider.class).to(NoneCachingHostAddressProvider.class).in(Scopes.SINGLETON);
                },
                new AbstractModule()
                {
                    @Provides
                    public DeltaLakeMetastore getDeltaLakeMetastore(@RawHiveMetastoreFactory HiveMetastoreFactory hiveMetastoreFactory)
                    {
                        return new HiveMetastoreBackedDeltaLakeMetastore(hiveMetastoreFactory.createMetastore(Optional.empty()));
                    }
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        deltaLakeMetadataFactory = injector.getInstance(DeltaLakeMetadataFactory.class);

        injector.getInstance(DeltaLakeMetastore.class)
                .createDatabase(Database.builder()
                        .setDatabaseName(DATABASE_NAME)
                        .setOwnerName(Optional.of("test"))
                        .setOwnerType(Optional.of(USER))
                        .setLocation(Optional.empty())
                        .build());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closeAll(() -> deleteRecursively(temporaryCatalogDirectory.toPath(), ALLOW_INSECURE));
        temporaryCatalogDirectory = null;
    }

    @Test
    public void testGetNewTableLayout()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        Optional<ConnectorTableLayout> newTableLayout = deltaLakeMetadata.getNewTableLayout(
                SESSION,
                newTableMetadata(
                        ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                        ImmutableList.of(BIGINT_COLUMN_2)));

        assertThat(newTableLayout).isPresent();

        // should not have ConnectorPartitioningHandle since DeltaLake does not support bucketing
        assertThat(newTableLayout.get().getPartitioning()).isNotPresent();

        assertThat(newTableLayout.get().getPartitionColumns())
                .isEqualTo(ImmutableList.of(BIGINT_COLUMN_2.getName()));

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetNewTableLayoutNoPartitionColumns()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        assertThat(deltaLakeMetadata.getNewTableLayout(
                SESSION,
                newTableMetadata(
                        ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                        ImmutableList.of())))
                .isNotPresent();

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetNewTableLayoutInvalidPartitionColumns()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        assertThatThrownBy(() -> deltaLakeMetadata.getNewTableLayout(
                SESSION,
                newTableMetadata(
                        ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                        ImmutableList.of(BIGINT_COLUMN_2, MISSING_COLUMN))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Table property 'partitioned_by' contained column names which do not exist: [missing_column]");

        assertThatThrownBy(() -> deltaLakeMetadata.getNewTableLayout(
                SESSION,
                newTableMetadata(
                        ImmutableList.of(TIMESTAMP_COLUMN, BIGINT_COLUMN_2),
                        ImmutableList.of(BIGINT_COLUMN_2))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Unsupported type: timestamp(3)");

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetInsertLayout()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of(BIGINT_COLUMN_1));

        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);

        Optional<ConnectorTableLayout> insertLayout = deltaLakeMetadata
                .getInsertLayout(
                        SESSION,
                        deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable()));

        assertThat(insertLayout).isPresent();

        assertThat(insertLayout.get().getPartitioning()).isNotPresent();

        assertThat(insertLayout.get().getPartitionColumns())
                .isEqualTo(getPartitionColumnNames(ImmutableList.of(BIGINT_COLUMN_1)));

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    private ConnectorTableMetadata newTableMetadata(List<ColumnMetadata> tableColumns, List<ColumnMetadata> partitionTableColumns)
    {
        return newTableMetadata(tableColumns, partitionTableColumns, false, NONE);
    }

    private ConnectorTableMetadata newTableMetadata(
            List<ColumnMetadata> tableColumns,
            List<ColumnMetadata> partitionTableColumns,
            boolean cdfEnabled,
            DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode)
    {
        return new ConnectorTableMetadata(
                newMockSchemaTableName(),
                tableColumns,
                ImmutableMap.of(
                        PARTITIONED_BY_PROPERTY,
                        getPartitionColumnNames(partitionTableColumns),
                        COLUMN_MAPPING_MODE_PROPERTY,
                        columnMappingMode.name(),
                        CHANGE_DATA_FEED_ENABLED_PROPERTY,
                        cdfEnabled));
    }

    @Test
    public void testGetInsertLayoutTableUnpartitioned()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1),
                ImmutableList.of());

        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);

        // should return empty insert layout since table exists but is unpartitioned
        assertThat(deltaLakeMetadata.getInsertLayout(
                SESSION,
                deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable())))
                .isNotPresent();

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testApplyProjection()
    {
        testApplyProjection(
                ImmutableSet.of(),
                SYNTHETIC_COLUMN_ASSIGNMENTS,
                SIMPLE_COLUMN_PROJECTIONS,
                SIMPLE_COLUMN_PROJECTIONS,
                ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                SYNTHETIC_COLUMN_ASSIGNMENTS);
        testApplyProjection(
                // table handle already contains subset of expected projected columns
                ImmutableSet.of(BOGUS_COLUMN_HANDLE),
                SYNTHETIC_COLUMN_ASSIGNMENTS,
                SIMPLE_COLUMN_PROJECTIONS,
                SIMPLE_COLUMN_PROJECTIONS,
                ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                SYNTHETIC_COLUMN_ASSIGNMENTS);
        testApplyProjection(
                // table handle already contains superset of expected projected columns
                ImmutableSet.of(DOUBLE_COLUMN_HANDLE, BOOLEAN_COLUMN_HANDLE, DATE_COLUMN_HANDLE, BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                SYNTHETIC_COLUMN_ASSIGNMENTS,
                SIMPLE_COLUMN_PROJECTIONS,
                SIMPLE_COLUMN_PROJECTIONS,
                ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                SYNTHETIC_COLUMN_ASSIGNMENTS);
        testApplyProjection(
                // table handle has empty assignments
                ImmutableSet.of(DOUBLE_COLUMN_HANDLE, BOOLEAN_COLUMN_HANDLE, DATE_COLUMN_HANDLE, BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                ImmutableMap.of(),
                SIMPLE_COLUMN_PROJECTIONS,
                SIMPLE_COLUMN_PROJECTIONS,
                ImmutableSet.of(),
                ImmutableMap.of());
        testApplyProjection(
                ImmutableSet.of(DOUBLE_COLUMN_HANDLE, BOOLEAN_COLUMN_HANDLE, DATE_COLUMN_HANDLE, BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                ImmutableMap.of(),
                DEREFERENCE_COLUMN_PROJECTIONS,
                DEREFERENCE_COLUMN_PROJECTIONS,
                ImmutableSet.of(),
                ImmutableMap.of());
        testApplyProjection(
                ImmutableSet.of(NESTED_COLUMN_HANDLE),
                NESTED_COLUMN_ASSIGNMENTS,
                NESTED_DEREFERENCE_COLUMN_PROJECTIONS,
                EXPECTED_NESTED_DEREFERENCE_COLUMN_PROJECTIONS,
                ImmutableSet.of(EXPECTED_NESTED_COLUMN_HANDLE),
                EXPECTED_NESTED_COLUMN_ASSIGNMENTS);
    }

    private void testApplyProjection(
            Set<DeltaLakeColumnHandle> inputProjectedColumns,
            Map<String, ColumnHandle> inputAssignments,
            List<ConnectorExpression> inputProjections,
            List<ConnectorExpression> expectedProjections,
            Set<DeltaLakeColumnHandle> expectedProjectedColumns,
            Map<String, ColumnHandle> expectedAssignments)
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        ProjectionApplicationResult<ConnectorTableHandle> projection = deltaLakeMetadata
                .applyProjection(
                        SESSION,
                        createDeltaLakeTableHandle(inputProjectedColumns, PREDICATE_COLUMNS),
                        inputProjections,
                        inputAssignments)
                .get();

        assertThat(((DeltaLakeTableHandle) projection.getHandle()).getProjectedColumns())
                .isEqualTo(Optional.of(expectedProjectedColumns));

        assertThat(projection.getProjections())
                .usingRecursiveComparison()
                .isEqualTo(expectedProjections);

        assertThat(projection.getAssignments())
                .usingRecursiveComparison()
                .isEqualTo(createNewColumnAssignments(expectedAssignments));

        assertThat(projection.isPrecalculateStatistics())
                .isFalse();

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testApplyProjectionWithEmptyResult()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        assertThat(deltaLakeMetadata
                .applyProjection(
                        SESSION,
                        createDeltaLakeTableHandle(
                                ImmutableSet.of(BOGUS_COLUMN_HANDLE, VARCHAR_COLUMN_HANDLE),
                                PREDICATE_COLUMNS),
                        SIMPLE_COLUMN_PROJECTIONS,
                        SYNTHETIC_COLUMN_ASSIGNMENTS))
                .isEmpty();

        assertThat(deltaLakeMetadata
                .applyProjection(
                        SESSION,
                        createDeltaLakeTableHandle(ImmutableSet.of(), ImmutableSet.of()),
                        ImmutableList.of(),
                        ImmutableMap.of()))
                .isEmpty();

        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetInputInfoForPartitionedTable()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of(BIGINT_COLUMN_1));
        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable());

        assertThat(deltaLakeMetadata.getInfo(tableHandle)).isEqualTo(Optional.of(
                new DeltaLakeInputInfoBuilder()
                        .setPartitioned(true)
                        .build()));
        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetInputInfoForUnPartitionedTable()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of());
        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable());

        assertThat(deltaLakeMetadata.getInfo(tableHandle)).isEqualTo(Optional.of(new DeltaLakeInputInfoBuilder().build()));
        deltaLakeMetadata.cleanupQuery(SESSION);
    }

    @Test
    public void testGetInputInfoForCdfEnabled()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of(),
                true,
                NONE);
        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable());

        assertThat(deltaLakeMetadata.getInfo(tableHandle)).isEqualTo(Optional.of(
                new DeltaLakeInputInfoBuilder()
                        .setCdfEnabled(true)
                        .build()));
    }

    @Test
    public void testGetInputInfoForCheckConstraintsPresent()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        MetadataEntry metadataEntry = new MetadataEntry(
                "test_id",
                "test_name",
                "test_description",
                new MetadataEntry.Format("test_provider", ImmutableMap.of()),
                """
                {"fields":[{"name":"birthDate","type":"timestamp","nullable":true,"metadata":{}}]}"
                """,
                ImmutableList.of("test_partition_column"),
                ImmutableMap.of(
                        "delta.constraints.test", "a>10",
                        "delta.constraints.test2", "b>20"),
                1);

        DeltaLakeTableHandle deltaLakeTableHandle = createDeltaLakeTableHandle(ImmutableSet.of(), ImmutableSet.of(), metadataEntry);

        assertThat(deltaLakeMetadata.getInfo(deltaLakeTableHandle)).isEqualTo(Optional.of(
                new DeltaLakeInputInfoBuilder()
                        .setPartitioned(true)
                        .setCheckConstraints("a>10,b>20")
                        .build()));
    }

    @Test
    public void testGetInputInfoForColumnMappingMode()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());
        ConnectorTableMetadata tableMetadata = newTableMetadata(
                ImmutableList.of(BIGINT_COLUMN_1, BIGINT_COLUMN_2),
                ImmutableList.of(),
                false,
                NAME);
        deltaLakeMetadata.createTable(SESSION, tableMetadata, FAIL);
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) deltaLakeMetadata.getTableHandle(SESSION, tableMetadata.getTable());
        assertThat(deltaLakeMetadata.getInfo(tableHandle)).isEqualTo(Optional.of(
                new DeltaLakeInputInfoBuilder()
                        .setColumnMappingMode("NAME")
                        .build()));
    }

    @Test
    public void testGetInputInfoForGeneratedColumns()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        MetadataEntry metadataEntry = new MetadataEntry(
                "test_id",
                "test_name",
                "test_description",
                new MetadataEntry.Format("test_provider", ImmutableMap.of()),
                """
                {"fields":[{"name":"birthDate","type":"timestamp","nullable":true,"metadata":{}},
                {"name":"dateOfBirth","type":"date","nullable":true,"metadata":{"delta.generationExpression":"CAST(birthDate AS DATE)"}}]}"
                """,
                ImmutableList.of("test_partition_column"),
                ImmutableMap.of(),
                1);

        DeltaLakeTableHandle deltaLakeTableHandle = createDeltaLakeTableHandle(ImmutableSet.of(), ImmutableSet.of(), metadataEntry);

        assertThat(deltaLakeMetadata.getInfo(deltaLakeTableHandle)).isEqualTo(Optional.of(
                new DeltaLakeInputInfoBuilder()
                        .setPartitioned(true)
                        .setNumberOfGeneratedColumns(1)
                        .build()));
    }

    @Test
    public void testGetInputInfoForPartitionGeneratedColumns()
    {
        DeltaLakeMetadata deltaLakeMetadata = deltaLakeMetadataFactory.create(SESSION.getIdentity());

        MetadataEntry metadataEntry = new MetadataEntry(
                "test_id",
                "test_name",
                "test_description",
                new MetadataEntry.Format("test_provider", ImmutableMap.of()),
                """
                {"fields":[{"name":"birthDate","type":"timestamp","nullable":true,"metadata":{}},
                {"name":"dateOfBirth","type":"date","nullable":true,"metadata":{"delta.generationExpression":"CAST(birthDate AS DATE)"}}]}"
                """,
                ImmutableList.of("dateOfBirth"),
                ImmutableMap.of(),
                1);

        DeltaLakeTableHandle deltaLakeTableHandle = createDeltaLakeTableHandle(ImmutableSet.of(), ImmutableSet.of(), metadataEntry);

        assertThat(deltaLakeMetadata.getInfo(deltaLakeTableHandle)).isEqualTo(Optional.of(
                new DeltaLakeInputInfoBuilder()
                        .setPartitioned(true)
                        .setNumberOfGeneratedColumns(1)
                        .setNumberOfPartitionGeneratedColumns(1)
                        .build()));
    }

    @Test
    public void testInputExtendedStatisticsNotRequested()
    {
        String tableName = "test_input_extended_statistics_not_requested_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS a", 1);

        inTransaction(session -> assertInputInfo(session, tableName,  new DeltaLakeInputInfoBuilder()
                .setExtendedStatisticsMetric("NOT_REQUESTED")
                .build())
        );

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInputExtendedStatisticsRequestedPresent()
    {
        String tableName = "test_input_extended_statistics_requested_present_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS a", 1);
        inTransaction(session -> {
            simulateStatisticsRequest(session, tableName);
            assertInputInfo(session, tableName,  new DeltaLakeInputInfoBuilder()
                    .setExtendedStatisticsMetric("REQUESTED_PRESENT")
                    .setNumberOfDataFiles(Optional.of(1))
                    .build());

        });
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInputExtendedStatisticsRequestedNotPresent()
    {
        String tableName = "test_input_extended_statistics_requested_not_present_" + randomNameSuffix();
        Session sessionWithExtendedStatisticsOnWriteDisabled = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().get(), EXTENDED_STATISTICS_COLLECT_ON_WRITE, "false")
                .build();
        assertUpdate(sessionWithExtendedStatisticsOnWriteDisabled, "CREATE TABLE " + tableName + " AS SELECT 1 AS a", 1);

        inTransaction(session -> {
            simulateStatisticsRequest(session, tableName);
            assertInputInfo(session, tableName,  new DeltaLakeInputInfoBuilder()
                    .setExtendedStatisticsMetric("REQUESTED_NOT_PRESENT")
                    .setNumberOfDataFiles(Optional.of(1))
                    .build());
        });

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testGetInputInfoForNumberOfDataFiles()
    {
        String tableName = "test_get_input_info_for_number_of_data_files_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES('txt1'), ('txt2')", 2);

        // Number of data files metric is not present until cache is loaded
        assertInputInfoNumberOfDataFiles(tableName, Optional.empty(), 1);

        assertQuerySucceeds("SELECT * FROM " + tableName); // Fill cache
        assertInputInfoNumberOfDataFiles(tableName, Optional.of(1), 1);

        assertUpdate("INSERT INTO " + tableName + " VALUES('txt3')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('txt3')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('txt3')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('txt3')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('txt3')", 1);
        assertQuerySucceeds("SELECT * FROM " + tableName); // Fill cache
        assertInputInfoNumberOfDataFiles(tableName, Optional.of(6), 6);

        assertUpdate("UPDATE " + tableName + " SET a = 'txt10' WHERE a = 'txt2'", 1);
        assertQuerySucceeds("SELECT * FROM " + tableName); // Fill cache
        assertInputInfoNumberOfDataFiles(tableName, Optional.of(7), 7);

        assertUpdate("DELETE FROM " + tableName, 7);
        assertQuerySucceeds("SELECT * FROM " + tableName); // Fill cache
        assertInputInfoNumberOfDataFiles(tableName, Optional.of(0), 8);
    }

    private void assertInputInfoNumberOfDataFiles(String tableName, Optional<Integer> numberOfDataFiles, long version)
    {
        newTransaction().execute(
                // Make sure that each call to getInfo is with different query_id to not read cached transactionLog
                Session.builder(super.getSession()).setQueryId(QueryId.valueOf(randomNameSuffix())).build(),
                (session) -> {
                     assertInputInfo(session, tableName, new DeltaLakeInputInfoBuilder()
                            .setNumberOfDataFiles(numberOfDataFiles)
                            .setVersion(version)
                            .build());
                }
        );
    }

    private void assertInputInfo(Session session, String tableName, DeltaLakeInputInfo deltaLakeInputInfo)
    {
        Metadata metadata = getQueryRunner().getPlannerContext().getMetadata();
        QualifiedObjectName qualifiedObjectName = new QualifiedObjectName(
                session.getCatalog().orElse(DELTA_CATALOG),
                session.getSchema().orElse("tpch"),
                tableName);
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, qualifiedObjectName);
        assertThat(tableHandle).isPresent();
        Optional<Object> tableInfo = metadata.getInfo(session, tableHandle.get());
        assertThat(tableInfo).isPresent();
        DeltaLakeInputInfo deltaInputInfo = (DeltaLakeInputInfo) tableInfo.get();
        assertThat(deltaInputInfo).isEqualTo(deltaLakeInputInfo);
    }

    private void simulateStatisticsRequest(Session session, String tableName)
    {
        Metadata metadata = getQueryRunner().getPlannerContext().getMetadata();
        QualifiedObjectName qualifiedObjectName = new QualifiedObjectName(
                session.getCatalog().orElse(DELTA_CATALOG),
                session.getSchema().orElse("tpch"),
                tableName);
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, qualifiedObjectName);
        metadata.getTableStatistics(session, tableHandle.orElseThrow());
    }

    private static DeltaLakeTableHandle createDeltaLakeTableHandle(Set<DeltaLakeColumnHandle> projectedColumns, Set<DeltaLakeColumnHandle> constrainedColumnsn)
    {
        return createDeltaLakeTableHandle(projectedColumns, constrainedColumnsn, createMetadataEntry());
    }

    private static DeltaLakeTableHandle createDeltaLakeTableHandle(
            Set<DeltaLakeColumnHandle> projectedColumns,
            Set<DeltaLakeColumnHandle> constrainedColumns,
            MetadataEntry metadataEntry)
    {
        return new DeltaLakeTableHandle(
                "test_schema_name",
                "test_table_name",
                true,
                "test_location",
                metadataEntry,
                new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()),
                createConstrainedColumnsTuple(constrainedColumns),
                TupleDomain.all(),
                Optional.of(DeltaLakeTableHandle.WriteType.UPDATE),
                Optional.of(projectedColumns),
                Optional.of(ImmutableList.of(BOOLEAN_COLUMN_HANDLE)),
                Optional.of(ImmutableList.of(DOUBLE_COLUMN_HANDLE)),
                Optional.empty(),
                0);
    }

    private static TupleDomain<DeltaLakeColumnHandle> createConstrainedColumnsTuple(
            Set<DeltaLakeColumnHandle> constrainedColumns)
    {
        ImmutableMap.Builder<DeltaLakeColumnHandle, Domain> tupleBuilder = ImmutableMap.builder();

        constrainedColumns.forEach(column -> {
            verify(column.isBaseColumn(), "Unexpected dereference: %s", column);
            tupleBuilder.put(column, Domain.notNull(column.getBaseType()));
        });

        return TupleDomain.withColumnDomains(tupleBuilder.buildOrThrow());
    }

    private static List<Assignment> createNewColumnAssignments(Map<String, ColumnHandle> assignments)
    {
        return assignments.entrySet().stream()
                .map(assignment -> {
                    DeltaLakeColumnHandle column = ((DeltaLakeColumnHandle) assignment.getValue());
                    Type type = column.getProjectionInfo().map(DeltaLakeColumnProjectionInfo::getType).orElse(column.getBaseType());
                    return new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            type);
                })
                .collect(toImmutableList());
    }

    private static MetadataEntry createMetadataEntry()
    {
        return new MetadataEntry(
                "test_id",
                "test_name",
                "test_description",
                new MetadataEntry.Format("test_provider", ImmutableMap.of()),
                "test_schema",
                ImmutableList.of("test_partition_column"),
                ImmutableMap.of("test_configuration_key", "test_configuration_value"),
                1);
    }

    private static List<String> getPartitionColumnNames(List<ColumnMetadata> tableMetadataColumns)
    {
        return tableMetadataColumns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());
    }

    private static SchemaTableName newMockSchemaTableName()
    {
        String randomSuffix = UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        return new SchemaTableName(DATABASE_NAME, "table_" + randomSuffix);
    }

    private static class DeltaLakeInputInfoBuilder
    {
        private boolean partitioned = false;
        private long version = 0;
        private boolean cdfEnabled = false;
        private String checkConstraints = "";
        private String columnMappingMode = "NONE";
        private int numberOfGeneratedColumns = 0;
        private int numberOfPartitionGeneratedColumns = 0;
        private String extendedStatisticsMetric = "NOT_REQUESTED";
        private Optional<Integer> numberOfDataFiles = Optional.empty();

        public DeltaLakeInputInfoBuilder setPartitioned(boolean partitioned)
        {
            this.partitioned = partitioned;
            return this;
        }

        public DeltaLakeInputInfoBuilder setVersion(long version)
        {
            this.version = version;
            return this;
        }

        public DeltaLakeInputInfoBuilder setCdfEnabled(boolean cdfEnabled)
        {
            this.cdfEnabled = cdfEnabled;
            return this;
        }

        public DeltaLakeInputInfoBuilder setCheckConstraints(String checkConstraints)
        {
            this.checkConstraints = checkConstraints;
            return this;
        }

        public DeltaLakeInputInfoBuilder setColumnMappingMode(String columnMappingMode)
        {
            this.columnMappingMode = columnMappingMode;
            return this;
        }

        public DeltaLakeInputInfoBuilder setNumberOfGeneratedColumns(int numberOfGeneratedColumns)
        {
            this.numberOfGeneratedColumns = numberOfGeneratedColumns;
            return this;
        }

        public DeltaLakeInputInfoBuilder setNumberOfPartitionGeneratedColumns(int numberOfPartitionGeneratedColumns)
        {
            this.numberOfPartitionGeneratedColumns = numberOfPartitionGeneratedColumns;
            return this;
        }

        public DeltaLakeInputInfoBuilder setExtendedStatisticsMetric(String extendedStatisticsMetric)
        {
            this.extendedStatisticsMetric = extendedStatisticsMetric;
            return this;
        }

        public DeltaLakeInputInfoBuilder setNumberOfDataFiles(Optional<Integer> numberOfDataFiles)
        {
            this.numberOfDataFiles = numberOfDataFiles;
            return this;
        }

        DeltaLakeInputInfo build()
        {
            ImmutableMap.Builder<String, String> galaxyTraitsBuilder = ImmutableMap.<String, String>builder()
                    .put("cdfEnabled", Boolean.toString(cdfEnabled))
                    .put("checkConstraints", checkConstraints)
                    .put("columnMappingMode", columnMappingMode)
                    .put("numberOfGeneratedColumns", Integer.toString(numberOfGeneratedColumns))
                    .put("numberOfPartitionGeneratedColumns", Integer.toString(numberOfPartitionGeneratedColumns))
                    .put("extendedStatisticsMetric", extendedStatisticsMetric);

            numberOfDataFiles.ifPresent(numberOfDataFiles -> galaxyTraitsBuilder.put("numberOfDataFilesInTable", Integer.toString(numberOfDataFiles)));

            return new DeltaLakeInputInfo(partitioned, galaxyTraitsBuilder.buildOrThrow(), version);
        }
    }
}
