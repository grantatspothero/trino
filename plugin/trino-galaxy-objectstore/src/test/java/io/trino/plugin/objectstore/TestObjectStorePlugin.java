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
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestObjectStorePlugin
{
    private static final String TESTING_ACCOUNT_URL = "https://whackadoodle.galaxy.com";

    @Test
    public void testCreateConnector()
    {
        ConnectorFactory factory = getConnectorFactory();

        // simplest possible configuration
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("OBJECTSTORE__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HIVE__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HIVE__hive.metastore.uri", "thrift://foo:1234")
                                .put("ICEBERG__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("ICEBERG__hive.metastore.uri", "thrift://foo:1234")
                                .put("DELTA__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("DELTA__hive.metastore.uri", "thrift://foo:1234")
                                .put("HUDI__galaxy.account-url", TESTING_ACCOUNT_URL)
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
                                .put("HIVE__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HIVE__hive.metastore", "glue")
                                .put("HIVE__hive.metastore.glue.region", "us-east-2")
                                .put("ICEBERG__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("ICEBERG__iceberg.catalog.type", "glue")
                                .put("ICEBERG__hive.metastore.glue.region", "us-east-2")
                                .put("DELTA__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("DELTA__hive.metastore", "glue")
                                .put("DELTA__hive.metastore.glue.region", "us-east-2")
                                .put("HUDI__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HUDI__hive.metastore", "glue")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(new ObjectStorePlugin().getConnectorFactories());
    }
}
