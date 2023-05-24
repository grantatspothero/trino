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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeTableProperties;
import io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore;
import io.trino.plugin.hive.HiveTestUtils;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.objectstore.S3Assert.s3Path;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for <a href="https://starburstdata.atlassian.net/browse/IN-916">Incident IN-916</a>,
 * see `#tmp-galaxy-inc-8150` on Slack.
 */
public class TestReadCorruptedTables
        extends AbstractTestQueryFramework
{
    private static final String PATH_SEPARATOR = "/";
    private static final String DEFAULT_DATA_DIRECTORY = "s3://galaxy-trino-ci-static/test_resources/"; // not relevant, since we're dealing with existing data sets

    private TestingGalaxyMetastore galaxyMetastore;
    private AmazonS3 s3;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String catalog = "iceberg";
        String schema = "test_double_slashes";

        GalaxyCockroachContainer cockroach = closeAfterClass(new GalaxyCockroachContainer());
        galaxyMetastore = closeAfterClass(new TestingGalaxyMetastore(cockroach));

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(catalog)
                                .setSchema(schema)
                                .build())
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", Map.of());

            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog(catalog, "galaxy_objectstore", createObjectStoreProperties(
                    new ObjectStoreConfig().getTableType(),
                    Map.of(
                            "galaxy.location-security.enabled", "false",
                            "galaxy.catalog-id", "c-1234567890",
                            "galaxy.account-url", "https://localhost:1234"),
                    galaxyMetastore.getMetastoreConfig(DEFAULT_DATA_DIRECTORY),
                    Map.of(),
                    Map.of()));

            queryRunner.execute("CREATE SCHEMA %s.%s".formatted(catalog, schema));

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @BeforeClass
    public void setUp()
    {
        s3 = AmazonS3ClientBuilder.standard()
                .withRegion("us-east-2") // must match bucket
                .build();
    }

    @BeforeClass
    public void registerTables()
    {
        String schema = getSession().getSchema().orElseThrow();

        HiveMetastore metastore = new GalaxyHiveMetastore(
                galaxyMetastore.getMetastore(),
                HiveTestUtils.HDFS_ENVIRONMENT,
                DEFAULT_DATA_DIRECTORY);

        // Register Iceberg tables: register_table-equivalent, but without any validation
        for (Map.Entry<String, IcebergTable> entry : Map.of(
                        "iceberg_double_slashes", new IcebergTable(
                                "s3://galaxy-trino-ci-static/test_resources//test_double_slashes/iceberg-b754219094244b30a6f3cd2904aeff4e",
                                "s3://galaxy-trino-ci-static/test_resources//test_double_slashes//iceberg-b754219094244b30a6f3cd2904aeff4e/metadata/00002-df018b61-9e0e-4263-b95d-476af06d1374.metadata.json"),
                        "iceberg_double_slashes_ctas", new IcebergTable(
                                "s3://galaxy-trino-ci-static/test_resources//test_double_slashes/iceberg_ctas-2e7f97526b6544de8d41cba5b5dfcb23",
                                "s3://galaxy-trino-ci-static/test_resources//test_double_slashes//iceberg_ctas-2e7f97526b6544de8d41cba5b5dfcb23/metadata/00001-16f7868e-eab4-4634-887b-2ebf1969d27c.metadata.json"))
                .entrySet()) {
            String tableName = entry.getKey();
            IcebergTable tableInfo = entry.getValue();
            metastore.createTable(
                    Table.builder()
                            .setDatabaseName(schema)
                            .setTableName(tableName)
                            .setTableType(EXTERNAL_TABLE.name())
                            .setParameter("EXTERNAL", "TRUE")
                            .setOwner(Optional.of("whoever"))
                            .addDataColumn(new Column("ignored", HIVE_INT, Optional.empty()))
                            .withStorage(storage -> storage
                                    .setLocation(tableInfo.location)
                                    .setStorageFormat(AbstractIcebergTableOperations.ICEBERG_METASTORE_STORAGE_FORMAT))
                            .setParameter(BaseMetastoreTableOperations.TABLE_TYPE_PROP, BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                            .setParameter(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, tableInfo.metadataLocation)
                            .build(),
                    PrincipalPrivileges.NO_PRIVILEGES);
        }

        // Register Delta tables: register_table-equivalent, but without any validation
        for (Map.Entry<String, String> entry : Map.of(
                        "delta_double_slashes", "s3://galaxy-trino-ci-static/test_resources//test_double_slashes/delta-b62aa2d3f88d464c8ef5d3ce6ee52ab1",
                        "delta_double_slashes_ctas", "s3://galaxy-trino-ci-static/test_resources//test_double_slashes/delta_ctas-8dbcfaea11a3431281a71704a2dbc8d8")
                .entrySet()) {
            String tableName = entry.getKey();
            String tableLocation = entry.getValue();
            metastore.createTable(
                    Table.builder()
                            .setDatabaseName(schema)
                            .setTableName(tableName)
                            .setTableType(EXTERNAL_TABLE.name())
                            .setOwner(Optional.of("whoever"))
                            .addDataColumn(new Column("ignored", HIVE_INT, Optional.empty()))
                            .setParameter(HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_PROPERTY, HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_VALUE)
                            .setParameter(DeltaLakeTableProperties.LOCATION_PROPERTY, tableLocation)
                            .withStorage(storage -> storage
                                    .setStorageFormat(DeltaLakeMetadata.DELTA_STORAGE_FORMAT)
                                    .setSerdeParameters(Map.of(DeltaLakeMetadata.PATH_PROPERTY, tableLocation))
                                    .setLocation(tableLocation))
                            .build(),
                    PrincipalPrivileges.NO_PRIVILEGES);
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (s3 != null) {
            s3.shutdown();
            s3 = null;
        }
        galaxyMetastore = null; // closed by closeAfterClass
    }

    @Test
    public void testReadCorruptedDeltaTable()
    {
        assertThat(query("TABLE delta_double_slashes"))
                .matches("VALUES (42, 12345)");

        String path = (String) computeScalar("SELECT DISTINCT \"$path\" FROM delta_double_slashes");
        assertThat(path)
                .isEqualTo("s3://galaxy-trino-ci-static/test_resources//test_double_slashes/delta-b62aa2d3f88d464c8ef5d3ce6ee52ab1/20230525_100346_39428_r9swe-7a2abbf5-1afe-446c-8c3c-0cdb7b1675f6");
        verifyCorrupted(path);
    }

    @Test
    public void testReadCorruptedDeltaTableCreatedWithCtas()
    {
        assertThat(query("TABLE delta_double_slashes_ctas"))
                .matches("VALUES (42, 12345)");

        String path = (String) computeScalar("SELECT DISTINCT \"$path\" FROM delta_double_slashes_ctas");
        assertThat(path)
                .isEqualTo("s3://galaxy-trino-ci-static/test_resources//test_double_slashes/delta_ctas-8dbcfaea11a3431281a71704a2dbc8d8/20230525_100330_39108_r9swe-86ae080a-de6f-41bd-a882-e354c031a9cd");
        verifyCorrupted(path);
    }

    @Test
    public void testReadCorruptedIcebergTable()
    {
        assertThat(query("TABLE iceberg_double_slashes"))
                .matches("VALUES (42, 12345)");

        String path = (String) computeScalar("SELECT DISTINCT \"$path\" FROM iceberg_double_slashes");
        assertThat(path)
                .isEqualTo("s3://galaxy-trino-ci-static/test_resources//test_double_slashes//iceberg-b754219094244b30a6f3cd2904aeff4e/data/20230525_100341_39373_r9swe-cb2d9e5b-ed8d-4f02-8129-d1382392aaaf.orc");
        verifyCorrupted(path);
    }

    @Test
    public void testReadCorruptedIcebergTableCreatedWithCtas()
    {
        assertThat(query("TABLE iceberg_double_slashes_ctas"))
                .matches("VALUES (42, 12345)");

        String path = (String) computeScalar("SELECT DISTINCT \"$path\" FROM iceberg_double_slashes_ctas");
        assertThat(path)
                .isEqualTo("s3://galaxy-trino-ci-static/test_resources//test_double_slashes//iceberg_ctas-2e7f97526b6544de8d41cba5b5dfcb23/data/20230525_100325_38972_r9swe-1fcfe23f-350c-4330-8d17-f6eea610a4d6.orc");
        verifyCorrupted(path);
    }

    private void verifyCorrupted(String path)
    {
        assertThat(s3Path(s3, path))
                .doesNotExist()
                .withKey(corruptedKey(path))
                .exists();
    }

    private record IcebergTable(String location, String metadataLocation)
    {
        IcebergTable
        {
            requireNonNull(location, "location is null");
            requireNonNull(metadataLocation, "metadataLocation is null");
        }
    }

    private static String corruptedKey(String s3Path)
    {
        checkArgument(s3Path.startsWith("s3://"));
        String key = keyFromPath(brokenHadoopPath(s3Path));
        assertThat(key)
                .contains(s3Path.replaceAll(".*/", "") + "#%2F");
        return key;
    }

    // Copied from https://github.com/trinodb/trino/blob/46e215294bb01917ddd2bd7ce085a2f2d2cad8a4/lib/trino-hdfs/src/main/java/io/trino/filesystem/hdfs/HadoopPaths.java#L28-L41
    private static Path brokenHadoopPath(String path)
    {
        // hack to preserve the original path for S3 if necessary
        Path hadoopPath = new Path(path);
        if ("s3".equals(hadoopPath.toUri().getScheme()) && !path.equals(hadoopPath.toString())) {
            if (hadoopPath.toUri().getFragment() != null) {
                throw new IllegalArgumentException("Unexpected URI fragment in path: " + path);
            }
            URI uri = URI.create(path);
            return new Path(uri + "#" + URLEncoder.encode(uri.getPath(), UTF_8));
        }
        return hadoopPath;
    }

    // Copied from https://github.com/trinodb/trino/blob/baeb9d5918f38c19bbe56de4497c42d737a6c4a0/lib/trino-hdfs/src/main/java/io/trino/hdfs/s3/TrinoS3FileSystem.java#L1021-L1035
    private static String keyFromPath(Path path)
    {
        checkArgument(path.isAbsolute(), "Path is not absolute: %s", path);
        // hack to use path from fragment -- see IcebergSplitSource#hadoopPath()
        String key = Optional.ofNullable(path.toUri().getFragment())
                .or(() -> Optional.ofNullable(path.toUri().getPath()))
                .orElse("");
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        if (key.endsWith(PATH_SEPARATOR)) {
            key = key.substring(0, key.length() - PATH_SEPARATOR.length());
        }
        return key;
    }
}
