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
package io.trino.plugin.warp2;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.tools.configuration.MultiPrefixConfigurationWrapper;
import org.testng.annotations.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestWarpSpeedPlugin
{
    private static final String TESTING_ACCOUNT_URL = "https://whackadoodle.galaxy.com";
    private static final String TESTING_CATALOG_ID = "c-1234567890";

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
                                .put("HIVE__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("HIVE__galaxy.catalog-id", TESTING_CATALOG_ID)
                                .put("HIVE__hive.metastore.uri", "thrift://foo:1234")
                                .put("ICEBERG__galaxy.account-url", TESTING_ACCOUNT_URL)
                                .put("ICEBERG__galaxy.catalog-id", TESTING_CATALOG_ID)
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
    public void testCreateConnectorIceberg()
    {
        ConnectorFactory factory = getConnectorFactory();

        // simplest possible configuration
        factory.create(
                        "test",
                        Map.of(
                                MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX + CloudVendorConfiguration.STORE_PATH, "s3://some-bucket/some-folder",
                                MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX + ProxiedConnectorConfiguration.PASS_THROUGH_DISPATCHER, "hive,hudi,delta-lake,iceberg",
                                MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX + ProxiedConnectorConfiguration.PROXIED_CONNECTOR, ProxiedConnectorConfiguration.ICEBERG_CONNECTOR_NAME,
                                "hive.metastore.uri", "thrift://foo:1234"),
                        new TestingConnectorContext())
                .shutdown();
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(WarpSpeedConnectorTestUtils.getPlugin().getConnectorFactories());
    }
}
