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

import com.google.inject.Module;
import io.trino.plugin.objectstore.InternalObjectStoreConnectorFactory;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.ProxiedConnectorInitializer;
import io.trino.plugin.warp.proxiedconnector.deltalake.DeltaLakeProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.hive.HiveProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.hudi.HudiProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.iceberg.IcebergProxiedConnectorTransformer;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ObjectStoreProxyConnectorInitializer
        implements ProxiedConnectorInitializer
{
    public static final String CONNECTOR_NAME = "galaxy_objectstore";

    @Override
    public List<Module> getModules(ConnectorContext context)
    {
        return List.of(
                binder -> {
                    binder.bind(DispatcherProxiedConnectorTransformer.class)
                            .to(ObjectStoreProxiedConnectorTransformer.class);

                    binder.bind(DeltaLakeProxiedConnectorTransformer.class);
                    binder.bind(HiveProxiedConnectorTransformer.class);
                    binder.bind(IcebergProxiedConnectorTransformer.class);
                    binder.bind(HudiProxiedConnectorTransformer.class);
                });
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Map<String, String> objectStoreConfig = config.entrySet()
                .stream()
                .filter(e -> !e.getKey().startsWith("warp-speed.") && !e.getKey().startsWith("WARP__"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Because of native code, Warp Speed does not support multiple catalogs and thus doesn't need classloader duplication.
        return InternalObjectStoreConnectorFactory.createConnector(
                catalogName,
                objectStoreConfig,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                binder -> {},
                context);
    }

    @Override
    public String getProxiedConnectorName()
    {
        return CONNECTOR_NAME;
    }
}
