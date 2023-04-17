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
import io.trino.plugin.hive.BaseTestHiveOnDataLake;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test certain Hive features like flush_metadata_cache procedure via Object Store connector.
 */
public class TestObjectStoreHiveOnDataLake
        extends BaseTestHiveOnDataLake
{
    public TestObjectStoreHiveOnDataLake()
    {
        super("whatever-this-is-unused");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        bucketName = "test-object-store-on-data-lake-" + randomNameSuffix();
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName, HiveHadoop.DEFAULT_IMAGE));
        hiveMinioDataLake.start();
        metastoreClient = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .build());

        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> true));
        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> galaxyCockroachContainer));

        return ObjectStoreQueryRunner.builder()
                .withCatalogName("hive")
                .withSchemaName("tpch") // as in superclass. Doesn't matter as HIVE_TEST_SCHEMA is used for test tables.
                .withTableType(TableType.HIVE)
                .withAccountClient(testingAccountFactory.createAccount())
                .withHiveS3Config(ImmutableMap.<String, String>builder()
                        .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("hive.s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                        .put("hive.s3.path-style-access", "true")
                        .buildOrThrow())
                .withLocationSecurityServer(locationSecurityServer)
                .withExtraObjectStoreProperties(ImmutableMap.<String, String>builder()
                        // Metastore
                        .put("HIVE__hive.metastore", "thrift")
                        .put("HIVE__hive.metastore.uri", "thrift://" + hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .put("ICEBERG__iceberg.catalog.type", "HIVE_METASTORE")
                        .put("ICEBERG__hive.metastore.uri", "thrift://" + hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .put("DELTA__hive.metastore", "thrift")
                        .put("DELTA__hive.metastore.uri", "thrift://" + hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .put("HUDI__hive.metastore", "thrift")
                        .put("HUDI__hive.metastore.uri", "thrift://" + hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        // Required for tests
                        .put("HIVE__hive.insert-existing-partitions-behavior", "OVERWRITE")
                        // .put("HIVE__hive.non-managed-table-writes-enabled", "true") // always enabled for ObjectStoreQueryRunner
                        // Below are required to enable caching on metastore (as enabled by the superclass)
                        .put("HIVE__hive.metastore-cache-ttl", "1d")
                        .put("HIVE__hive.metastore-refresh-interval", "1d")
                        .put("DELTA__hive.metastore-cache-ttl", "1d")
                        .put("DELTA__hive.metastore-refresh-interval", "1d")
                        .put("HUDI__hive.metastore-cache-ttl", "1d")
                        .put("HUDI__hive.metastore-refresh-interval", "1d")
                        // This is required to enable AWS Athena partition projection
                        .put("HIVE__hive.partition-projection-enabled", "true")
                        .buildOrThrow())
                .build();
    }

    @BeforeClass
    public void grantAccessToTestSchema()
    {
        computeActual("GRANT ALL PRIVILEGES ON hive.\"%s\".\"*\" TO ROLE %s WITH GRANT OPTION".formatted(HIVE_TEST_SCHEMA, ACCOUNT_ADMIN));
    }

    @Override
    public void testDatePartitionProjectionFormatTextWillNotCauseIntervalRequirement()
    {
        assertThatThrownBy(super::testDatePartitionProjectionFormatTextWillNotCauseIntervalRequirement)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testDatePartitionProjectionOnDateColumnWithDefaults()
    {
        assertThatThrownBy(super::testDatePartitionProjectionOnDateColumnWithDefaults)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testDatePartitionProjectionOnTimestampColumnWithInterval()
    {
        assertThatThrownBy(super::testDatePartitionProjectionOnTimestampColumnWithInterval)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testDatePartitionProjectionOnTimestampColumnWithIntervalExpressionCreatedOnTrino()
    {
        assertThatThrownBy(super::testDatePartitionProjectionOnTimestampColumnWithIntervalExpressionCreatedOnTrino)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testDatePartitionProjectionOnVarcharColumnWithDaysInterval()
    {
        assertThatThrownBy(super::testDatePartitionProjectionOnVarcharColumnWithDaysInterval)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testDatePartitionProjectionOnVarcharColumnWithHoursInterval()
    {
        assertThatThrownBy(super::testDatePartitionProjectionOnVarcharColumnWithHoursInterval)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testDatePartitionProjectionOnVarcharColumnWithIntervalExpression()
    {
        assertThatThrownBy(super::testDatePartitionProjectionOnVarcharColumnWithIntervalExpression)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testEnumPartitionProjectionOnVarcharColumnWithWhitespace()
    {
        assertThatThrownBy(super::testEnumPartitionProjectionOnVarcharColumnWithWhitespace)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testEnumPartitionProjectionOnVarcharColumnWithStorageLocationTemplateCreatedOnTrino()
    {
        assertThatThrownBy(super::testEnumPartitionProjectionOnVarcharColumnWithStorageLocationTemplateCreatedOnTrino)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testEnumPartitionProjectionOnVarcharColumn()
    {
        assertThatThrownBy(super::testEnumPartitionProjectionOnVarcharColumn)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testIntegerPartitionProjectionOnVarcharColumnWithDigitsAlignCreatedOnTrino()
    {
        assertThatThrownBy(super::testIntegerPartitionProjectionOnVarcharColumnWithDigitsAlignCreatedOnTrino)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testIntegerPartitionProjectionOnIntegerColumnWithInterval()
    {
        assertThatThrownBy(super::testIntegerPartitionProjectionOnIntegerColumnWithInterval)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testIntegerPartitionProjectionOnIntegerColumnWithDefaults()
    {
        assertThatThrownBy(super::testIntegerPartitionProjectionOnIntegerColumnWithDefaults)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testInjectedPartitionProjectionOnVarcharColumn()
    {
        assertThatThrownBy(super::testInjectedPartitionProjectionOnVarcharColumn)
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Catalog 'hive' column property 'partition_projection_type' does not exist");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testPartitionProjectionInvalidTableProperties()
    {
        assertThatThrownBy(super::testPartitionProjectionInvalidTableProperties)
                .hasMessageStartingWith("""

                        Expecting message to be:
                          "Partition projection can't be defined for non partition column: 'name'"
                        but was:
                          "Catalog 'hive' column property 'partition_projection_type' does not exist\"""");
        throw new SkipException("partition projection not supported");
    }

    @Override
    public void testInsertOverwriteInTransaction()
    {
        assertThatThrownBy(super::testInsertOverwriteInTransaction)
                .hasMessageStartingWith("""

                        Expecting message to be:
                          "Overwriting existing partition in non auto commit context doesn't support DIRECT_TO_TARGET_EXISTING_DIRECTORY write mode"
                        but was:
                          "Catalog only supports writes using autocommit: hive\"""");
    }
}
