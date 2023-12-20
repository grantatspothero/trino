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
package io.trino.plugin.snowflake;

import com.google.inject.Injector;
import com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeJdbcOverrideModule;
import com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeParallelConnector;
import com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeParallelModule;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.jdbc.JdbcModule;
import io.trino.spi.NodeManager;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.Map;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeConnectorFlavour.PARALLEL;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class GalaxySnowflakeParallelConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return PARALLEL.getName();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                binder -> binder.bind(TypeManager.class).toInstance(context.getTypeManager()),
                binder -> binder.bind(NodeManager.class).toInstance(context.getNodeManager()),
                binder -> binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder()),
                binder -> binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName)),
                binder -> binder.bind(CatalogHandle.class).toInstance(context.getCatalogHandle()),
                new JdbcModule(),
                new SnowflakeParallelModule(),
                new SnowflakeJdbcOverrideModule(),
                new GalaxySnowflakeParallelModule(),
                binder -> {
                    configBinder(binder).bindConfig(GalaxySnowflakeConfig.class);
                    configBinder(binder).bindConfig(LocalRegionConfig.class);
                    configBinder(binder).bindConfig(CrossRegionConfig.class);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(SnowflakeParallelConnector.class);
    }
}
