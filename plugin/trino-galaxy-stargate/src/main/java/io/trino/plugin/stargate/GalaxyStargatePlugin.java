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
package io.trino.plugin.stargate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugin.stargate.EnableWrites;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class GalaxyStargatePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return getConnectorFactories(false);
    }

    @VisibleForTesting
    Iterable<ConnectorFactory> getConnectorFactories(boolean enableWrites)
    {
        return ImmutableList.of(getConnectorFactory(enableWrites));
    }

    private ConnectorFactory getConnectorFactory(boolean enableWrites)
    {
        return new JdbcConnectorFactory(
                // "stargate" will be used also for the parallel variant, with implementation chosen by a configuration property
                "stargate",
                combine(
                        binder -> binder.bind(Boolean.class).annotatedWith(EnableWrites.class).toInstance(enableWrites),
                        binder -> binder.install(new ExtraCredentialsBasedIdentityCacheMappingModule()),
                        new GalaxyStargateModule()));
    }
}
