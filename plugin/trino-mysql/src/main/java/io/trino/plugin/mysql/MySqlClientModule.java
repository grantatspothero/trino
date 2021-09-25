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
package io.trino.plugin.mysql;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.mysql.cj.jdbc.Driver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.galaxy.RegionEnforcementConfig;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.plugin.mysql.galaxy.GalaxyMySqlSocketFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;

import java.sql.SQLException;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.mysql.galaxy.GalaxyMySqlSocketFactory.addCatalogId;
import static io.trino.plugin.mysql.galaxy.GalaxyMySqlSocketFactory.addCatalogName;
import static io.trino.plugin.mysql.galaxy.GalaxyMySqlSocketFactory.addCrossRegionAllowed;
import static io.trino.plugin.mysql.galaxy.GalaxyMySqlSocketFactory.addRegionLocalIpAddresses;
import static io.trino.plugin.mysql.galaxy.GalaxyMySqlSocketFactory.addSshTunnelProperties;

public class MySqlClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(MySqlClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(MySqlJdbcConfig.class);
        configBinder(binder).bindConfig(MySqlConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            MySqlConfig mySqlConfig,
            RegionEnforcementConfig regionEnforcementConfig,
            SshTunnelConfig sshTunnelConfig)
            throws SQLException
    {
        Properties properties = getConnectionProperties(mySqlConfig);
        properties.setProperty("socketFactory", GalaxyMySqlSocketFactory.class.getName());
        addCatalogName(properties, catalogHandle.getCatalogName());
        addCatalogId(properties, catalogHandle.getVersion().toString());
        addCrossRegionAllowed(properties, false);
        addRegionLocalIpAddresses(properties, regionEnforcementConfig.getAllowedIpAddresses());

        SshTunnelProperties.generateFrom(sshTunnelConfig)
                .ifPresent(sshTunnelProperties -> addSshTunnelProperties(properties, sshTunnelProperties));

        return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), properties, credentialProvider);
    }

    public static Properties getConnectionProperties(MySqlConfig mySqlConfig)
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("useInformationSchema", Boolean.toString(mySqlConfig.isDriverUseInformationSchema()));
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        connectionProperties.setProperty("rewriteBatchedStatements", "true");

        // Try to make MySQL timestamps work (See https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-time-instants.html)
        // without relying on server time zone (which may be configured to be totally unusable).
        // TODO (https://github.com/trinodb/trino/issues/15668) rethink how timestamps are mapped. Also, probably worth adding tests
        //  with MySQL server with a non-UTC system zone.
        connectionProperties.setProperty("connectionTimeZone", "UTC");

        if (mySqlConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(mySqlConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(mySqlConfig.getMaxReconnects()));
        }
        if (mySqlConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(mySqlConfig.getConnectionTimeout().toMillis()));
        }
        return connectionProperties;
    }
}
