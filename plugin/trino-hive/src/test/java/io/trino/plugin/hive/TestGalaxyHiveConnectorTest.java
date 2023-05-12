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
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGalaxyHiveConnectorTest
        extends TestHiveConnectorTest
{
    private TestingGalaxyMetastore testingGalaxyMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        GalaxyCockroachContainer cockroach = closeAfterClass(new GalaxyCockroachContainer());
        testingGalaxyMetastore = new TestingGalaxyMetastore(cockroach);

        Function<DistributedQueryRunner, HiveMetastore> metastore = queryRunner -> {
            File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
            return new GalaxyHiveMetastore(testingGalaxyMetastore.getMetastore(), HDFS_ENVIRONMENT, baseDir.getAbsolutePath());
        };

        // Use a compression codec that's fast and doesn't cause GCLocker
        verify(new HiveConfig().getHiveCompressionCodec() == HiveCompressionOption.DEFAULT);
        String hiveCompressionCodec = HiveCompressionCodec.ZSTD.name();

        DistributedQueryRunner queryRunner = HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.compression-codec", hiveCompressionCodec)
                        .put("hive.allow-register-partition-procedure", "true")
                        // Reduce writer sort buffer size to ensure SortingFileWriter gets used
                        .put("hive.writer-sort-buffer-size", "1MB")
                        .put("hive.security", "allow-all")
                        .buildOrThrow())
                .setInitialTables(ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION))
                .setTpchBucketedCatalogEnabled(true)
                .setMetastore(metastore)
                .build();

        // extra catalog with NANOSECOND timestamp precision
        queryRunner.createCatalog(
                "hive_timestamp_nanos",
                "hive",
                ImmutableMap.of("hive.timestamp-precision", "NANOSECONDS"));
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp()
            throws Exception
    {
        if (testingGalaxyMetastore != null) {
            testingGalaxyMetastore.close();
            testingGalaxyMetastore = null;
        }
    }

    @Test(enabled = false)
    @Override
    public void testCreateSchemaWithLongName() {}

    @Test(enabled = false)
    @Override
    public void testRenameSchemaToLongName() {}

    @Test(enabled = false)
    @Override
    public void testCreateTableWithLongTableName() {}

    @Test(enabled = false)
    @Override
    public void testRenameTableToLongTableName() {}

    @Test(enabled = false)
    @Override
    public void testAlterTableAddLongColumnName() {}

    @Test(enabled = false)
    @Override
    public void testShowCreateSchema() {}

    @Test(enabled = false)
    @Override
    public void testSchemaAuthorizationForUser() {}

    @Test(enabled = false)
    @Override
    public void testSchemaAuthorizationForRole() {}

    @Test(enabled = false)
    @Override
    public void testCurrentUserInView() {}

    @Test(enabled = false)
    @Override
    public void testCreateSchemaWithAuthorizationForUser() {}

    @Test(enabled = false)
    @Override
    public void testCreateSchemaWithAuthorizationForRole() {}

    @Test(enabled = false)
    @Override
    public void testSchemaAuthorization() {}

    @Test(enabled = false)
    @Override
    public void testShowColumnMetadata() {}

    @Test(enabled = false)
    @Override
    public void testShowTablePrivileges() {}

    @Test(enabled = false)
    @Override
    public void testTableAuthorization() {}

    @Test(enabled = false)
    @Override
    public void testTableAuthorizationForRole() {}

    @Test(enabled = false)
    @Override
    public void testTemporaryStagingDirectorySessionProperties() {}

    @Test(enabled = false)
    @Override
    public void testViewAuthorization() {}

    @Test(enabled = false)
    @Override
    public void testViewAuthorizationSecurityDefiner() {}

    @Test(enabled = false)
    @Override
    public void testViewAuthorizationSecurityInvoker() {}

    @Test(enabled = false)
    @Override
    public void testViewAuthorizationForRole() {}

    @Override
    public void testCreateAcidTableUnsupported()
    {
        assertThatThrownBy(super::testCreateAcidTableUnsupported)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("\n" +
                        "Expecting message:\n" +
                        "  \"GalaxyHiveMetastore does not support ACID tables\"\n" +
                        "to match regex:\n" +
                        "  \"FileHiveMetastore does not support ACID tables\"\n" +
                        "but did not.");

        assertQueryFails("CREATE TABLE acid_unsupported (x int) WITH (transactional = true)", "GalaxyHiveMetastore does not support ACID tables");
        assertQueryFails("CREATE TABLE acid_unsupported WITH (transactional = true) AS SELECT 123 x", "GalaxyHiveMetastore does not support ACID tables");
    }

    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        testCreateSchemaWithNonLowercaseOwnerNameOriginalTest();
    }
}
