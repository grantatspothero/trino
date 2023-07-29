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
package io.trino.plugin.warp;

import io.trino.plugin.objectstore.TestObjectStoreHudiConnectorTest;
import io.trino.spi.Plugin;
import io.trino.testing.QueryRunner;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestWarpSpeedObjectStoreHudiConnectorTest
        extends TestObjectStoreHudiConnectorTest
{
    @Override
    protected Plugin getObjectStorePlugin()
    {
        return WarpSpeedConnectorTestUtils.getPlugin();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(
                HUDI,
                WarpSpeedConnectorTestUtils.getCoordinatorProperties(),
                WarpSpeedConnectorTestUtils.getProperties());
    }

    @Override
    public void testColumnCommentMaterializedView()
    {
        Properties properties = new Properties();
        try (InputStream inputStream = getResource(getClass(), "test-warpspeed-plugin-versions.properties").openStream()) {
            properties.load(inputStream);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        String galaxyTrinoVersion = properties.getProperty("project.version");
        String baseTrinoVersion = galaxyTrinoVersion.replaceFirst("-galaxy-1-SNAPSHOT$", "");

        // TODO io.trino.plugin.varada.dispatcher.DispatcherMetadata needs to implement ConnectorMetadata.setMaterializedViewColumnComment
        //  which should be part of 423 update; ConnectorMetadata.setMaterializedViewColumnComment was added in 423.
        //  when this assertion fails and we are on 423+, we should remove the whole test override
        assertEquals(baseTrinoVersion, "422");

        assertThatThrownBy(super::testColumnCommentMaterializedView)
                .hasMessageStartingWith("""

                        Expecting message to be:
                          "updateMaterializedViewColumnComment is not supported for Iceberg Galaxy catalogs"
                        but was:
                          "This connector does not support setting materialized view column comments\"""");
    }
}
