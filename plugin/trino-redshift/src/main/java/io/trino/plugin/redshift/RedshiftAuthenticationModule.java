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
package io.trino.plugin.redshift;

import com.amazon.redshift.Driver;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.SingletonIdentityCacheMapping;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;

import java.util.Properties;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.redshift.RedshiftAuthenticationConfig.RedshiftAuthenticationType.AWS;
import static io.trino.plugin.redshift.RedshiftAuthenticationConfig.RedshiftAuthenticationType.PASSWORD;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;
import static java.util.Objects.requireNonNull;

public class RedshiftAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                RedshiftAuthenticationConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                new PasswordModule()));

        install(conditionalModule(
                RedshiftAuthenticationConfig.class,
                config -> config.getAuthenticationType() == AWS,
                new AwsModule()));
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
            binder.bind(IdentityCacheMapping.class).to(SingletonIdentityCacheMapping.class).in(Scopes.SINGLETON);
        }

        @Singleton
        @Provides
        @ForBaseJdbc
        public static ConnectionFactory getConnectionFactory(
                CatalogHandle catalogHandle,
                BaseJdbcConfig config,
                CredentialProvider credentialProvider,
                LocalRegionConfig localRegionConfig,
                CrossRegionConfig crossRegionConfig,
                SshTunnelConfig sshTunnelConfig,
                OpenTelemetry openTelemetry)
        {
            return new DriverConnectionFactory(
                    new Driver(),
                    config.getConnectionUrl(),
                    getDriverProperties(catalogHandle, localRegionConfig, crossRegionConfig, sshTunnelConfig),
                    credentialProvider,
                    openTelemetry);
        }
    }

    private static class AwsModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(IdentityCacheMapping.class).to(SingletonIdentityCacheMapping.class).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(RedshiftAwsCredentialsConfig.class);
            binder.bind(CredentialProvider.class).to(RedshiftAwsCredentialProvider.class).in(SINGLETON);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(
                CatalogHandle catalogHandle,
                BaseJdbcConfig config,
                RedshiftAwsCredentialsConfig awsCredentialsConfig,
                CredentialProvider credentialProvider,
                LocalRegionConfig localRegionConfig,
                CrossRegionConfig crossRegionConfig,
                SshTunnelConfig sshTunnelConfig,
                OpenTelemetry openTelemetry)
        {
            return new DriverConnectionFactory(
                    new Driver(),
                    config.getConnectionUrl(),
                    getConnectionProperties(catalogHandle, awsCredentialsConfig, localRegionConfig, crossRegionConfig, sshTunnelConfig),
                    credentialProvider,
                    openTelemetry);
        }

        private static Properties getConnectionProperties(
                CatalogHandle catalogHandle,
                RedshiftAwsCredentialsConfig awsCredentialsConfig,
                LocalRegionConfig localRegionConfig,
                CrossRegionConfig crossRegionConfig,
                SshTunnelConfig sshTunnelConfig)
        {
            requireNonNull(awsCredentialsConfig, "awsCredentialsConfig is null");
            Properties properties = new Properties();

            properties.put("Region", awsCredentialsConfig.getRegionName());
            properties.put("AccessKeyID", awsCredentialsConfig.getAccessKey());
            properties.put("SecretAccessKey", awsCredentialsConfig.getSecretKey());
            properties.putAll(getDriverProperties(catalogHandle, localRegionConfig, crossRegionConfig, sshTunnelConfig));

            return properties;
        }
    }

    private static Properties getDriverProperties(CatalogHandle catalogHandle, LocalRegionConfig localRegionConfig, CrossRegionConfig crossRegionConfig, SshTunnelConfig sshTunnelConfig)
    {
        Properties properties = new Properties();
        properties.put("reWriteBatchedInserts", "true");
        properties.put("reWriteBatchedInsertsSize", "512");

        properties.put("socketFactory", GalaxySqlSocketFactory.class.getName());
        RegionVerifierProperties.addRegionVerifierProperties(properties::setProperty, RegionVerifierProperties.generateFrom(localRegionConfig, crossRegionConfig));

        SshTunnelProperties.generateFrom(sshTunnelConfig)
                .ifPresent(sshTunnelProperties -> addSshTunnelProperties(properties::setProperty, sshTunnelProperties));

        CatalogNetworkMonitorProperties.addCatalogNetworkMonitorProperties(properties::setProperty, CatalogNetworkMonitorProperties.generateFrom(crossRegionConfig, catalogHandle));

        return properties;
    }
}