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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import io.trino.plugin.varada.dispatcher.CachingPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;

public class WarpSpeedPlugin
        implements Plugin
{
    private final CachingPlugin cachingPlugin;

    public WarpSpeedPlugin()
    {
        this(null, null);
    }

    @VisibleForTesting
    WarpSpeedPlugin(Module storageEngineModule, Module amazonModule)
    {
        cachingPlugin = new CachingPlugin();
        cachingPlugin.withStorageEngineModule(storageEngineModule);
        cachingPlugin.withAmazonModule(amazonModule);
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return List.of(new WarpSpeedConnectorFactory(cachingPlugin.getConnectorFactory()));
    }
}
