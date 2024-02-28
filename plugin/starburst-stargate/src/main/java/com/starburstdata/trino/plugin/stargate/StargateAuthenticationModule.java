/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
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
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;

import java.util.Optional;
import java.util.Properties;

import static com.starburstdata.trino.plugin.stargate.StargateConfig.PASSWORD;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;

public class StargateAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                StargateConfig.class,
                config -> PASSWORD.equalsIgnoreCase(config.getAuthenticationType()),
                new PasswordModule()));
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(StargateCredentialConfig.class);
            binder.bind(StargateCatalogIdentityFactory.class)
                    .to(PasswordCatalogIdentityFactory.class)
                    .in(Scopes.SINGLETON);
        }

        @Provides
        @Singleton
        @TransportConnectionFactory
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
