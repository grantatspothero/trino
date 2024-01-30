/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.sap.db.jdbc.Driver;
import com.starburstdata.trino.plugin.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.trino.plugin.jdbc.PoolingConnectionFactory;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;

public class SapHanaAuthenticationModule
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
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            configBinder(binder).bindConfig(JdbcConnectionPoolConfig.class);
            install(new ExtraCredentialsBasedIdentityCacheMappingModule());
        }

        @Provides
        @Singleton
        @DefaultSapHanaBinding
        public ConnectionFactory getConnectionFactory(
                CatalogHandle catalogHandle,
                LocalRegionConfig localRegionConfig,
                CrossRegionConfig crossRegionConfig,
                SshTunnelConfig sshConfig,
                CatalogName catalogName,
                BaseJdbcConfig config,
                JdbcConnectionPoolConfig poolConfig,
                CredentialProvider credentialProvider,
                IdentityCacheMapping identityCacheMapping)
        {
            checkArgument(!poolConfig.isConnectionPoolEnabled(), "Galaxy SAP HANA connector does not support connection pooling");
            if (poolConfig.isConnectionPoolEnabled()) {
                return new PoolingConnectionFactory(catalogName.toString(), Driver.class, config, poolConfig, credentialProvider, identityCacheMapping);
            }
            Properties socketFactoryProperties = buildSocketFactoryProperties(catalogHandle, localRegionConfig, crossRegionConfig, sshConfig);
            return new GalaxySapHanaConnectionFactory(new Driver(), config.getConnectionUrl(), socketFactoryProperties, new DefaultCredentialPropertiesProvider(credentialProvider));
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

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @BindingAnnotation
    public @interface DefaultSapHanaBinding {}
}
