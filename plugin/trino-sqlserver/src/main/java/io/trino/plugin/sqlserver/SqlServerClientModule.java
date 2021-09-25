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
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.galaxy.RegionEnforcementConfig;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.plugin.sqlserver.galaxy.GalaxySqlServerSocketFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogId;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogName;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCrossRegionAllowed;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addRegionLocalIpAddresses;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static io.trino.plugin.sqlserver.SqlServerClient.SQL_SERVER_MAX_LIST_EXPRESSIONS;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SqlServerClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(SqlServerConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SqlServerClient.class).in(Scopes.SINGLETON);
        bindTablePropertiesProvider(binder, SqlServerTableProperties.class);
        bindSessionPropertiesProvider(binder, SqlServerSessionProperties.class);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SQL_SERVER_MAX_LIST_EXPRESSIONS);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
        install(new JdbcJoinPushdownSupportModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            SqlServerConfig sqlServerConfig,
            RegionEnforcementConfig regionEnforcementConfig,
            SshTunnelConfig sshTunnelConfig,
            CredentialProvider credentialProvider)
    {
        Properties socketArgs = new Properties();
        addCatalogName(socketArgs, catalogHandle.getCatalogName());
        addCatalogId(socketArgs, catalogHandle.getVersion().toString());
        addCrossRegionAllowed(socketArgs, false);
        addRegionLocalIpAddresses(socketArgs, regionEnforcementConfig.getAllowedIpAddresses());
        SshTunnelProperties.generateFrom(sshTunnelConfig)
                .ifPresent(sshTunnelProperties -> addSshTunnelProperties(socketArgs::setProperty, sshTunnelProperties));

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

        DriverConnectionFactory delegate = new DriverConnectionFactory(new SQLServerDriver(), config.getConnectionUrl(), connectionProperties, credentialProvider);
        return new SqlServerConnectionFactory(delegate, sqlServerConfig.isSnapshotIsolationDisabled());
    }
}
