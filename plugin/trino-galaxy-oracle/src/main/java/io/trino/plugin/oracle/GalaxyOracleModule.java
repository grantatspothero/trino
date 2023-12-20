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

package io.trino.plugin.oracle;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.trino.plugin.license.LicenseManager;
import com.starburstdata.trino.plugin.oracle.PasswordAuthenticationConnectionProvider;
import com.starburstdata.trino.plugin.oracle.StarburstOracleClientModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;
import io.trino.sshtunnel.SshTunnelPropertiesMapper;
import oracle.jdbc.driver.OracleDriver;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Verify.verify;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.trino.plugin.oracle.UserPasswordModule.getConnectionProperties;

public class GalaxyOracleModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new StarburstOracleClientModule(new NoopLicenseManager()));

        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setBinding()
                .to(Key.get(ConnectionFactory.class, ForOracle.class))
                .in(Scopes.SINGLETON);
    }

    private static class NoopLicenseManager
            implements LicenseManager
    {
        @Override
        public boolean hasLicense()
        {
            return true;
        }
    }

    @Provides
    @Singleton
    @ForOracle
    public static ConnectionFactory createConnectionFactory(
            CatalogName catalogName,
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            OracleConfig oracleConfig,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig,
            SshTunnelConfig sshTunnelConfig)
    {
        Properties properties = getConnectionProperties(oracleConfig);

        // TODO: implement the catalog network monitor for cross-region support
        verify(!crossRegionConfig.getAllowCrossRegionAccess(), "Cross-region access not supported");
        RegionVerifierProperties.addRegionVerifierProperties(properties::setProperty, RegionVerifierProperties.generateFrom(localRegionConfig, crossRegionConfig));
        SshTunnelProperties.generateFrom(sshTunnelConfig)
                .ifPresent(sshTunnelProperties -> SshTunnelPropertiesMapper.addSshTunnelProperties(properties::setProperty, sshTunnelProperties));

        if (oracleConfig.isConnectionPoolEnabled()) {
            return new GalaxyOraclePoolingConnectionFactory(
                    catalogName,
                    config,
                    properties,
                    Optional.of(credentialProvider),
                    new PasswordAuthenticationConnectionProvider(),
                    oracleConfig);
        }

        return new GalaxyOracleDriverConnectionFactory(
                new OracleDriver(),
                config.getConnectionUrl(),
                properties,
                credentialProvider);
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @BindingAnnotation
    public @interface ForOracle {}
}
