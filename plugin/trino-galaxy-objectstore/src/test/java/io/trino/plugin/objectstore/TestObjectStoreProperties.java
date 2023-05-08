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
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestObjectStoreProperties
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "objectstore";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String schema = "default";
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(CATALOG)
                                .setSchema(schema)
                                .build())
                .build();
        try {
            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            String metastoreDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toString();
            queryRunner.createCatalog(CATALOG, "galaxy_objectstore", ImmutableMap.<String, String>builder()
                    // Hive
                    .put("HIVE__hive.metastore", "file")
                    .put("HIVE__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("HIVE__galaxy.location-security.enabled", "false")
                    .put("HIVE__galaxy.account-url", "https://localhost:1234")
                    .put("HIVE__galaxy.catalog-id", "c-1234567890")
                    // Iceberg
                    .put("ICEBERG__iceberg.catalog.type", "TESTING_FILE_METASTORE")
                    .put("ICEBERG__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("ICEBERG__galaxy.location-security.enabled", "false")
                    .put("ICEBERG__galaxy.account-url", "https://localhost:1234")
                    .put("ICEBERG__galaxy.catalog-id", "c-1234567890")
                    // Delta
                    .put("DELTA__hive.metastore", "file")
                    .put("DELTA__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("DELTA__galaxy.location-security.enabled", "false")
                    .put("DELTA__galaxy.account-url", "https://localhost:1234")
                    .put("DELTA__galaxy.catalog-id", "c-1234567890")
                    // Hudi
                    .put("HUDI__hive.metastore", "file")
                    .put("HUDI__hive.metastore.catalog.dir", metastoreDirectory)
                    .put("HUDI__galaxy.location-security.enabled", "false")
                    .put("HUDI__galaxy.account-url", "https://localhost:1234")
                    .put("HUDI__galaxy.catalog-id", "c-1234567890")
                    // ObjectStore
                    .put("OBJECTSTORE__object-store.table-type", TableType.ICEBERG.name())
                    .put("OBJECTSTORE__galaxy.location-security.enabled", "false")
                    .put("OBJECTSTORE__galaxy.account-url", "https://localhost:1234")
                    .put("OBJECTSTORE__galaxy.catalog-id", "c-1234567890")
                    .buildOrThrow());

            queryRunner.execute("CREATE SCHEMA %s.%s".formatted(CATALOG, schema));
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testSessionProperties()
    {
        assertThat(query("SHOW SESSION LIKE '" + CATALOG + ".%'"))
                .skippingTypesCheck()
                .exceptColumns("Description")
                // Name, Value, Default, Type
                // TODO test all, i.e. with matches()
                .containsAll("""
                        VALUES
                          -- ObjectStore single-delegate-specific property (Hive's)
                            ('objectstore.s3_select_pushdown_enabled', 'false', 'false', 'boolean')
                          -- ObjectStore property with common value across delegate connectors
                          , ('objectstore.statistics_enabled', 'true', 'true', 'boolean')
                          , ('objectstore.parquet_writer_batch_size', '10000', '10000', 'integer')
                          -- ObjectStore property that have, had or may have special handling
                          , ('objectstore.compression_codec', 'DEFAULT', 'DEFAULT', 'varchar')
                          , ('objectstore.parquet_optimized_writer_enabled', 'true', 'true', 'boolean')
                          , ('objectstore.projection_pushdown_enabled', 'true', 'true', 'boolean')
                        """);
    }
}
