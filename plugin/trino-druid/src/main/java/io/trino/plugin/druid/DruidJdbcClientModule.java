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
package io.trino.plugin.druid;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.plugin.base.galaxy.RegionEnforcementConfig;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;
import org.apache.calcite.avatica.remote.GalaxyDruidDriver;

import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogId;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogName;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addTlsEnabled;
import static io.trino.plugin.base.galaxy.RegionVerifier.addCrossRegionAllowed;
import static io.trino.plugin.base.galaxy.RegionVerifier.addRegionLocalIpAddresses;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;
import static java.util.Locale.ENGLISH;
import static org.apache.calcite.avatica.remote.GalaxyDruidDriver.CONNECT_STRING_PREFIX;

public class DruidJdbcClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(DruidJdbcClient.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory createConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            RegionEnforcementConfig regionEnforcementConfig,
            SshTunnelConfig sshTunnelConfig)
    {
        Properties galaxyProperties = new Properties();

        addCatalogName(galaxyProperties, catalogHandle.getCatalogName());
        addCatalogId(galaxyProperties, catalogHandle.getVersion().toString());
        addRegionLocalIpAddresses(galaxyProperties, regionEnforcementConfig.getAllowedIpAddresses());
        addCrossRegionAllowed(galaxyProperties, regionEnforcementConfig.getAllowCrossRegionAccess());

        if (config.getConnectionUrl().toLowerCase(ENGLISH).startsWith(CONNECT_STRING_PREFIX + "url=https")) {
            addTlsEnabled(galaxyProperties);
        }
        SshTunnelProperties.generateFrom(sshTunnelConfig)
                .ifPresent(sshTunnelProperties -> addSshTunnelProperties(galaxyProperties::setProperty, sshTunnelProperties));

        return new DriverConnectionFactory(
                new GalaxyDruidDriver(galaxyProperties),
                config.getConnectionUrl(),
                new Properties(),
                credentialProvider);
    }
}
