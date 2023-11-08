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
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

@Deprecated // TODO https://github.com/starburstdata/galaxy-trino/issues/890 Remove galaxy.metastore.batch-fetch.enabled
public class TestObjectStoreGalaxyMetastoreBatchFetchDisabled
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "objectstore";
    private static final String SCHEMA_NAME = "test_batch_fetch_disabled";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path schemaDirectory = createTempDirectory(null);
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());

        TestingGalaxyMetastore galaxyMetastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                testSessionBuilder()
                        .setCatalog(CATALOG_NAME)
                        .setSchema(SCHEMA_NAME)
                        .build())
                .build();

        queryRunner.installPlugin(new ObjectStorePlugin());

        queryRunner.createCatalog(CATALOG_NAME, "galaxy_objectstore", createObjectStoreProperties(
                new ObjectStoreConfig().getTableType(),
                Map.of(
                        "galaxy.location-security.enabled", "false",
                        "galaxy.catalog-id", "c-1234567890",
                        "galaxy.account-url", "https://localhost:1234"),
                "galaxy",
                ImmutableMap.<String, String>builder()
                        .put("galaxy.metastore.batch-fetch.enabled", "false")
                        .putAll(galaxyMetastore.getMetastoreConfig(schemaDirectory.toUri().toString()))
                        .buildOrThrow(),
                ImmutableMap.of(),
                Map.of()));

        queryRunner.execute("CREATE SCHEMA %s.%s WITH (location = '%s')".formatted(CATALOG_NAME, SCHEMA_NAME, schemaDirectory.toUri().toString()));
        return queryRunner;
    }

    @Test
    public void testGetAllTables()
    {
        String tableName = "test_table" + randomNameSuffix();
        String viewName = "test_view" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(col int)");
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

        assertThat(computeActual("SELECT table_name FROM information_schema.tables").getOnlyColumnAsSet())
                .contains(tableName)
                .contains(viewName);

        assertUpdate("DROP VIEW " + viewName);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testGetAllViews()
    {
        String tableName = "test_table" + randomNameSuffix();
        String viewName = "test_view" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(col int)");
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

        assertThat(computeActual("SELECT table_name FROM information_schema.views").getOnlyColumnAsSet())
                .contains(viewName)
                .doesNotContain(tableName);

        assertUpdate("DROP VIEW " + viewName);
        assertUpdate("DROP TABLE " + tableName);
    }
}
