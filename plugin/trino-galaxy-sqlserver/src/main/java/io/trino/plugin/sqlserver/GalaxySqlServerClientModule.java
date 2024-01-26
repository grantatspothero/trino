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
package io.trino.plugin.sqlserver;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.starburstdata.trino.plugin.license.LicenseManager;
import com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerClientModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Properties;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.trino.plugin.sqlserver.CatalogOverridingModule.ForCatalogOverriding;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class GalaxySqlServerClientModule
        extends AbstractConfigurationAwareModule
{
    private final LicenseManager licenseManager;

    public GalaxySqlServerClientModule(LicenseManager licenseManager)
    {
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new StarburstSqlServerClientModule(licenseManager));
        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForCatalogOverriding.class))
                .setBinding()
                .to(Key.get(ConnectionFactory.class, ForGalaxy.class))
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForGalaxy
    public static ConnectionFactory getConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            SqlServerConfig sqlServerConfig,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig,
            SshTunnelConfig sshTunnelConfig,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry)
    {
        Properties socketArgs = new Properties();
        RegionVerifierProperties.addRegionVerifierProperties(socketArgs::setProperty, RegionVerifierProperties.generateFrom(localRegionConfig, crossRegionConfig));
        SshTunnelProperties.generateFrom(sshTunnelConfig)
                .ifPresent(sshTunnelProperties -> addSshTunnelProperties(socketArgs::setProperty, sshTunnelProperties));

        CatalogNetworkMonitorProperties.addCatalogNetworkMonitorProperties(socketArgs::setProperty, CatalogNetworkMonitorProperties.generateFrom(crossRegionConfig, catalogHandle));

        Properties connectionProperties = new Properties();
        connectionProperties.put("socketFactoryClass", GalaxySqlServerSocketFactory.class.getName());

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            socketArgs.store(outputStream, null);
            // serialize ssh tunnel and region enforcement properties
            connectionProperties.put("socketFactoryConstructorArg", outputStream.toString(UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not construct SocketFactory argument", e);
        }

        DriverConnectionFactory delegate = new DriverConnectionFactory(new SQLServerDriver(), config.getConnectionUrl(), connectionProperties, credentialProvider, openTelemetry);
        return new SqlServerConnectionFactory(delegate, sqlServerConfig.isSnapshotIsolationDisabled());
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @BindingAnnotation
    public @interface ForGalaxy {}
}
