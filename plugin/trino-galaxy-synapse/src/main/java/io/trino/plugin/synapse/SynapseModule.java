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

package io.trino.plugin.synapse;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.starburstdata.trino.plugins.synapse.StarburstSynapseModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.galaxy.RegionEnforcementConfig;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.sqlserver.SqlServerClientModule;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sshtunnel.SshTunnelConfig;

import javax.inject.Qualifier;
import javax.inject.Singleton;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;

public class SynapseModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new StarburstSynapseModule());

        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setBinding()
                .to(Key.get(ConnectionFactory.class, ForSynapse.class))
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForSynapse
    public static ConnectionFactory synapseConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            SqlServerConfig sqlServerConfig,
            SshTunnelConfig sshConfig,
            RegionEnforcementConfig regionConfig,
            CredentialProvider credentials)
    {
        return SqlServerClientModule.getConnectionFactory(catalogHandle, config, sqlServerConfig, regionConfig, sshConfig, credentials);
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Qualifier
    public @interface ForSynapse {}
}
