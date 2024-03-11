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
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.TableHandle;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TestIcebergCacheSubqueriesTest;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.testing.QueryRunner;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.trino.plugin.varada.configuration.GlobalConfiguration.ENABLE_DEFAULT_WARMING;
import static io.trino.plugin.varada.configuration.GlobalConfiguration.LOCAL_STORE_PATH;
import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.ICEBERG_CONNECTOR_NAME;
import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.PROXIED_CONNECTOR;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestWarpSpeedIcebergCacheSubqueriesTest
        extends TestIcebergCacheSubqueriesTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path localStorePath = Files.createTempDirectory("local_store_");
        localStorePath.toFile().deleteOnExit();
        Path metastoreDirectory = Files.createTempDirectory("warp_iceberg_test");
        metastoreDirectory.toFile().deleteOnExit();
        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        return IcebergQueryRunner.builder()
                .setExtraProperties(EXTRA_PROPERTIES)
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "hive.metastore.catalog.dir", metastoreDirectory.toUri().toString(),
                        WARP_SPEED_PREFIX + "config.bundle-size-mb", "128",
                        WARP_SPEED_PREFIX + ENABLE_DEFAULT_WARMING, "false",
                        WARP_SPEED_PREFIX + CloudVendorConfiguration.STORE_PATH, "file:/" + localStorePath.toAbsolutePath(),
                        WARP_SPEED_PREFIX + LOCAL_STORE_PATH, localStorePath.toAbsolutePath().toString(),
                        WARP_SPEED_PREFIX + PROXIED_CONNECTOR, ICEBERG_CONNECTOR_NAME))
                .setInitialTables(REQUIRED_TABLES)
                .setIcebergPlugin(WarpSpeedConnectorTestUtils.getPlugin())
                .setIcebergConnectorName("warp_speed_2")
                .setFunctions(functions.build())
                .build();
    }

    @Override
    protected void createPartitionedTableAsSelect(String tableName, List<String> partitionColumns, String asSelect)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s WITH (partitioning=array[%s]) as %s",
                tableName,
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")),
                asSelect);

        getQueryRunner().execute(sql);
    }

    @Override
    protected IcebergTableHandle getIcebergTableHandle(TableHandle tableHandle)
    {
        return (IcebergTableHandle) ((DispatcherTableHandle) tableHandle.getConnectorHandle()).getProxyConnectorTableHandle();
    }

    @Override
    protected boolean effectivePredicateReturnedPerSplit()
    {
        return false;
    }

    @Override
    protected boolean getUnenforcedPredicateIsPrune()
    {
        return true;
    }
}
