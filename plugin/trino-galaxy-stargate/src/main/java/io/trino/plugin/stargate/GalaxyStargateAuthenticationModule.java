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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.jdbc.TrinoDriver;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;

import java.util.Optional;
import java.util.Properties;

import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;

public class GalaxyStargateAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new PasswordModule());
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder) {}

        @Provides
        @Singleton
        @GalaxyTransportConnectionFactory
        public ConnectionFactory getConnectionFactory(
                CatalogHandle catalogHandle,
                BaseJdbcConfig config,
                LocalRegionConfig localRegionConfig,
                CrossRegionConfig crossRegionConfig,
                SshTunnelConfig sshConfig,
                CredentialProvider credentialProvider)
        {
            Properties properties = new Properties();
            Properties socketFactoryProperties = buildSocketFactoryProperties(catalogHandle, localRegionConfig, crossRegionConfig, sshConfig);
            return new DriverConnectionFactory(new TrinoDriver(new GalaxySqlSocketFactory(socketFactoryProperties)), config.getConnectionUrl(), properties, credentialProvider);
        }
    }

    private static Properties buildSocketFactoryProperties(
            CatalogHandle catalogHandle,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig,
            SshTunnelConfig sshConfig)
    {
        Properties properties = new Properties();
        RegionVerifierProperties.addRegionVerifierProperties(properties::setProperty, RegionVerifierProperties.generateFrom(localRegionConfig, crossRegionConfig));

        Optional<SshTunnelProperties> sshTunnelProperties = SshTunnelProperties.generateFrom(sshConfig);
        sshTunnelProperties.ifPresent(sshProps -> addSshTunnelProperties(properties::setProperty, sshProps));

        // Do not enable TLS for secured clusters as plain socket is wrapped into SSLSocket in OkHttpClient library
        CatalogNetworkMonitorProperties catalogNetworkMonitorProperties = CatalogNetworkMonitorProperties.generateFrom(crossRegionConfig, catalogHandle);
        CatalogNetworkMonitorProperties.addCatalogNetworkMonitorProperties(properties::setProperty, catalogNetworkMonitorProperties);

        return properties;
    }
}
