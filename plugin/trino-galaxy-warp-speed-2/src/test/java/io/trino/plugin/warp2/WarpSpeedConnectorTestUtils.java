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

import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.di.VaradaStubsStorageEngineModule;
import io.trino.spi.Plugin;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.tools.configuration.MultiPrefixConfigurationWrapper;

import java.util.Map;

import static io.trino.plugin.warp2.WarpSpeedConnectorFactory.WARP_PREFIX;

public abstract class WarpSpeedConnectorTestUtils
{
    public static Plugin getPlugin()
    {
        WarpSpeedPlugin warpSpeedPlugin = new WarpSpeedPlugin();
        warpSpeedPlugin.withStorageEngineModule(new VaradaStubsStorageEngineModule());

        return warpSpeedPlugin;
    }

    public static Map<String, String> getProperties()
    {
        return Map.of(
                WARP_PREFIX + MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX + CloudVendorConfiguration.STORE_PATH, "s3://some-bucket/some-folder",
                WARP_PREFIX + MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX + GlobalConfiguration.LOCAL_STORE_PATH, "/tmp/",
                WARP_PREFIX + MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX + ProxiedConnectorConfiguration.PASS_THROUGH_DISPATCHER, "hive,hudi,delta-lake,iceberg");
    }

    public static Map<String, String> getCoordinatorProperties()
    {
        // warp_speed connector currently doesn't support coordinator scheduling
        return Map.of("node-scheduler.include-coordinator", "false");
    }
}
