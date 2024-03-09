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
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestObjectStorePlugin
{
    private static final String TESTING_ACCOUNT_URL = "https://whackadoodle.galaxy.com";
    private static final String TESTING_GALAXY_METASTORE_URL = "https://whackadoodle.galaxy.com";
    private static final String TESTING_GALAXY_METASTORE_ID = "ms-1234567890";
    private static final String TESTING_GALAXY_DEFAULT_DATA_DIR = "/dev/null";
    private static final String TESTING_CATALOG_ID = "c-1234567890";
    private static final String TESTING_CLUSTER_ID = "w-9999999999";
    private static final String SHARED_SECRET = "1234567890123456789012345678901234567890123456789012345678901234";

    @Test
    public void testCreateConnector()
    {
        ConnectorFactory factory = getConnectorFactory();

        // simplest possible configuration
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("OBJECTSTORE__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("OBJECTSTORE__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("OBJECTSTORE__galaxy.cluster-id", TESTING_CLUSTER_ID)
                                .put("HIVE__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HIVE__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("HIVE__hive.metastore.uri", "thrift://foo:1234")
                                .put("ICEBERG__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("ICEBERG__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("ICEBERG__galaxy.cluster-id", TESTING_CLUSTER_ID)
                                .put("ICEBERG__hive.metastore.uri", "thrift://foo:1234")
                                .put("DELTA__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("DELTA__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("DELTA__hive.metastore.uri", "thrift://foo:1234")
                                .put("HUDI__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HUDI__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("HUDI__hive.metastore.uri", "thrift://foo:1234")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testGlueMetastore()
    {
        ConnectorFactory factory = getConnectorFactory();

        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("OBJECTSTORE__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("OBJECTSTORE__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("OBJECTSTORE__galaxy.cluster-id", TESTING_CLUSTER_ID)
                                .put("HIVE__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HIVE__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("HIVE__hive.metastore", "glue")
                                .put("HIVE__hive.metastore.glue.region", "us-east-2")
                                .put("ICEBERG__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("ICEBERG__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("ICEBERG__galaxy.cluster-id", TESTING_CLUSTER_ID)
                                .put("ICEBERG__iceberg.catalog.type", "glue")
                                .put("ICEBERG__hive.metastore.glue.region", "us-east-2")
                                .put("DELTA__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("DELTA__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("DELTA__hive.metastore", "glue")
                                .put("DELTA__hive.metastore.glue.region", "us-east-2")
                                .put("HUDI__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HUDI__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("HUDI__hive.metastore", "glue")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testGalaxyMetastore()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("OBJECTSTORE__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("OBJECTSTORE__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("OBJECTSTORE__galaxy.cluster-id", TESTING_CLUSTER_ID)
                                .put("HIVE__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HIVE__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("HIVE__hive.metastore", "galaxy")
                                .put("HIVE__galaxy.metastore.default-data-dir", TESTING_GALAXY_DEFAULT_DATA_DIR)
                                .put("HIVE__galaxy.metastore.metastore-id", TESTING_GALAXY_METASTORE_ID)
                                .put("HIVE__galaxy.metastore.shared-secret", SHARED_SECRET)
                                .put("HIVE__galaxy.metastore.server-uri", TESTING_GALAXY_METASTORE_URL)
                                .put("ICEBERG__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("ICEBERG__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("ICEBERG__galaxy.cluster-id", TESTING_CLUSTER_ID)
                                .put("ICEBERG__iceberg.catalog.type", "GALAXY_METASTORE")
                                .put("ICEBERG__galaxy.metastore.default-data-dir", TESTING_GALAXY_DEFAULT_DATA_DIR)
                                .put("ICEBERG__galaxy.metastore.metastore-id", TESTING_GALAXY_METASTORE_ID)
                                .put("ICEBERG__galaxy.metastore.shared-secret", SHARED_SECRET)
                                .put("ICEBERG__galaxy.metastore.server-uri", TESTING_GALAXY_METASTORE_URL)
                                .put("DELTA__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("DELTA__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("DELTA__hive.metastore", "galaxy")
                                .put("DELTA__galaxy.metastore.default-data-dir", TESTING_GALAXY_DEFAULT_DATA_DIR)
                                .put("DELTA__galaxy.metastore.metastore-id", TESTING_GALAXY_METASTORE_ID)
                                .put("DELTA__galaxy.metastore.shared-secret", SHARED_SECRET)
                                .put("DELTA__galaxy.metastore.server-uri", TESTING_GALAXY_METASTORE_URL)
                                .put("HUDI__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HUDI__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("HUDI__hive.metastore", "galaxy")
                                .put("HUDI__galaxy.metastore.default-data-dir", TESTING_GALAXY_DEFAULT_DATA_DIR)
                                .put("HUDI__galaxy.metastore.metastore-id", TESTING_GALAXY_METASTORE_ID)
                                .put("HUDI__galaxy.metastore.shared-secret", SHARED_SECRET)
                                .put("HUDI__galaxy.metastore.server-uri", TESTING_GALAXY_METASTORE_URL)
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testCreateConnectorFailsWithUnusedConfig()
    {
        assertCreateConnectorFails("DELTA__unused_config", "somevalue", "Configuration property 'unused_config' was not used");
        assertCreateConnectorFails("HIVE__unused_config", "somevalue", "Configuration property 'unused_config' was not used");
        assertCreateConnectorFails("ICEBERG__unused_config", "somevalue", "Configuration property 'unused_config' was not used");
        assertCreateConnectorFails("NOTEXISTS__hive.metastore.uri", "somevalue", "Unused config: NOTEXISTS__hive.metastore.uri");
    }

    private static void assertCreateConnectorFails(String key, String value, String exceptionString)
    {
        ConnectorFactory factory = getConnectorFactory();

        assertThatThrownBy(() -> factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put(key, value)
                                .putAll(withAllTableTypes("hive.metastore.uri", "thrift://localhost:1234"))
                                .putAll(withAllTableTypes("galaxy.account-url", "https://localhost:1234"))
                                .putAll(withAllTableTypes("galaxy.catalog-id", "c-1234567890"))
                                .put("OBJECTSTORE__galaxy.cluster-id", "w-9999999999")
                                .put("ICEBERG__galaxy.cluster-id", "w-9999999999")
                                .buildOrThrow(),
                        new TestingConnectorContext()))
                .hasMessageContaining(exceptionString);
    }

    private static Map<String, String> withAllTableTypes(String key, String value)
    {
        return ImmutableMap.<String, String>builder()
                .put("HIVE__" + key, value)
                .put("ICEBERG__" + key, value)
                .put("DELTA__" + key, value)
                .put("HUDI__" + key, value)
                .buildOrThrow();
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(new ObjectStorePlugin().getConnectorFactories());
    }
}
