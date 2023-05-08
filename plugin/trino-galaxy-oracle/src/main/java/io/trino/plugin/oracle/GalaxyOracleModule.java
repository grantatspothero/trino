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
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.trino.plugins.oracle.OraclePoolingConnectionFactory;
import com.starburstdata.trino.plugins.oracle.StarburstOracleClientModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.galaxy.RegionEnforcementConfig;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.CatalogHandle;
import oracle.jdbc.driver.OracleDriver;

import javax.inject.Qualifier;
import javax.inject.Singleton;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Optional;
import java.util.Properties;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.trino.plugins.oracle.StarburstOracleClientModule.getConnectionProperties;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogId;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogName;

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
            CatalogHandle catalogHandle,
            CatalogName catalogName,
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            OracleConfig oracleConfig,
            RegionEnforcementConfig regionEnforcementConfig)
    {
        Properties properties = getConnectionProperties(oracleConfig);
        // TODO: add region enforcement and SSH tunneling support
        addCatalogName(properties, catalogHandle.getCatalogName());
        addCatalogId(properties, catalogHandle.getVersion().toString());

        if (oracleConfig.isConnectionPoolEnabled()) {
            return new OraclePoolingConnectionFactory(
                    catalogName,
                    config,
                    properties,
                    Optional.of(credentialProvider),
                    oracleConfig);
        }

        return new DriverConnectionFactory(
                new OracleDriver(),
                config.getConnectionUrl(),
                properties,
                credentialProvider);
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Qualifier
    public @interface ForOracle {}
}
