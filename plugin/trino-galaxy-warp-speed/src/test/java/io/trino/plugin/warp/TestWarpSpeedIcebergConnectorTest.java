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

import io.trino.plugin.objectstore.TestObjectStoreIcebergConnectorTest;
import io.trino.spi.Plugin;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestWarpSpeedIcebergConnectorTest
        extends TestObjectStoreIcebergConnectorTest
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
                ICEBERG,
                WarpSpeedConnectorTestUtils.getCoordinatorProperties(),
                WarpSpeedConnectorTestUtils.getProperties());
    }

    @Override
    public void testDropTableCorruptStorage()
    {
        // TODO IcebergProxiedConnectorTransformer.getSchemaTableName needs to handle CorruptedIcebergTableHandle
        assertThatThrownBy(super::testDropTableCorruptStorage)
                .isInstanceOf(QueryFailedException.class)
                .hasStackTraceContaining("class io.trino.plugin.iceberg.CorruptedIcebergTableHandle cannot be cast to class io.trino.plugin.iceberg.IcebergTableHandle (io.trino.plugin.iceberg.CorruptedIcebergTableHandle and io.trino.plugin.iceberg.IcebergTableHandle are in unnamed module of loader 'app')")
                .hasStackTraceContaining("at io.trino.plugin.warp.proxiedconnector.iceberg.IcebergProxiedConnectorTransformer.getSchemaTableName(IcebergProxiedConnectorTransformer.java");
    }
}
