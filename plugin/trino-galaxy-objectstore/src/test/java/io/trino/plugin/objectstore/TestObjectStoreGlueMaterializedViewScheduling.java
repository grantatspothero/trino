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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;

import java.util.Map;

import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;

public class TestObjectStoreGlueMaterializedViewScheduling
        extends BaseObjectStoreMaterializedViewSchedulingTest
{
    @Override
    protected DistributedQueryRunner setupQueryRunner()
            throws Exception
    {
        Map<String, String> properties = createCatalogProperties(Map.of("ICEBERG__iceberg.scheduled-materialized-view-refresh-enabled", "true"));
        Map<String, String> propertiesWithoutScheduling = createCatalogProperties(Map.of());

        return GalaxyQueryRunner.builder(TEST_CATALOG, "default")
                .setAccountClient(galaxyTestHelper.getAccountClient())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new ObjectStorePlugin())
                .addCatalog(TEST_CATALOG, "galaxy_objectstore", false, properties)
                .addCatalog(TEST_CATALOG_WITHOUT_SCHEDULING, "galaxy_objectstore", false, propertiesWithoutScheduling)
                .addPlugin(new TpchPlugin())
                .addCatalog("tpch", "tpch", true, ImmutableMap.of())
                .amendSession(session -> session.setCatalog(TEST_CATALOG).setSchema(schemaName))
                .build();
    }

    @Override
    protected String createSchemaSql(String catalogName, String schemaName)
    {
        return "CREATE SCHEMA %s.%s".formatted(catalogName, schemaName);
    }

    private Map<String, String> createCatalogProperties(Map<String, String> additionalProperties)
    {
        return createObjectStoreProperties(
            ICEBERG,
            ImmutableMap.<String, String>builder()
                    .put("galaxy.account-url", accountWorkService.getAccountUrl())
                    .put("galaxy.catalog-id", "c-1234567890")
                    .buildOrThrow(),
            "glue",
            ImmutableMap.of("hive.metastore.glue.default-warehouse-dir", "s3://galaxy-trino-ci/default"),
            ImmutableMap.of(),
            additionalProperties);
    }
}
