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
package io.trino.plugin.mariadb;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.plugin.base.galaxy.RegionEnforcementConfig;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.plugin.mariadb.galaxy.GalaxyMariaDbSocketFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;
import io.trino.sshtunnel.SshTunnelPropertiesMapper;
import org.mariadb.jdbc.Driver;

import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogId;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogName;
import static io.trino.plugin.base.galaxy.RegionVerifier.addCrossRegionAllowed;
import static io.trino.plugin.base.galaxy.RegionVerifier.addRegionLocalIpAddresses;

public class MariaDbClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(MariaDbClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(MariaDbJdbcConfig.class);
        binder.install(new DecimalModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            RegionEnforcementConfig regionEnforcementConfig,
            SshTunnelConfig sshTunnelConfig)
    {
        Properties properties = getConnectionProperties();
        properties.setProperty("socketFactory", GalaxyMariaDbSocketFactory.class.getName());

        addCatalogName(properties, catalogHandle.getCatalogName());
        addCatalogId(properties, catalogHandle.getVersion().toString());
        addCrossRegionAllowed(properties, regionEnforcementConfig.getAllowCrossRegionAccess());
        addRegionLocalIpAddresses(properties, regionEnforcementConfig.getAllowedIpAddresses());

        SshTunnelProperties.generateFrom(sshTunnelConfig)
                .ifPresent(sshTunnelProperties -> SshTunnelPropertiesMapper.addSshTunnelProperties(properties::setProperty, sshTunnelProperties));

        return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), properties, credentialProvider);
    }

    private static Properties getConnectionProperties()
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("tinyInt1isBit", "false");
        return connectionProperties;
    }
}
