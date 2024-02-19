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
package io.trino.plugin.clickhouse;

import com.clickhouse.jdbc.ClickHouseDriver;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.ConfigBinder;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.plugin.clickhouse.galaxy.GalaxyClickHouseSocketFactory;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;

import java.util.Properties;

import static com.clickhouse.client.config.ClickHouseClientOption.CUSTOM_SOCKET_FACTORY;
import static com.clickhouse.client.config.ClickHouseClientOption.CUSTOM_SOCKET_FACTORY_OPTIONS;
import static com.clickhouse.client.config.ClickHouseClientOption.USE_BINARY_STRING;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties.addCatalogNetworkMonitorProperties;
import static io.trino.plugin.clickhouse.ClickHouseClient.DEFAULT_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.clickhouse.galaxy.GalaxyClickhouseUtils.serializeProperties;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;

public class ClickHouseClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        ConfigBinder.configBinder(binder).bindConfig(ClickHouseConfig.class);
        bindSessionPropertiesProvider(binder, ClickHouseSessionProperties.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(ClickHouseClient.class).in(Scopes.SINGLETON);
        bindTablePropertiesProvider(binder, ClickHouseTableProperties.class);
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setDomainCompactionThreshold(DEFAULT_DOMAIN_COMPACTION_THRESHOLD));
        binder.install(new DecimalModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig,
            SshTunnelConfig sshTunnelConfig)
    {
        Properties properties = new Properties();

        Properties regionEnforcementAndTunnelProperties = new Properties();

        // Clickhouse driver expects the comma separated additional properties
        RegionVerifierProperties.addRegionVerifierProperties(regionEnforcementAndTunnelProperties::setProperty, RegionVerifierProperties.generateFrom(localRegionConfig, crossRegionConfig));

        SshTunnelProperties.generateFrom(sshTunnelConfig)
                .ifPresent(sshTunnelProperties -> addSshTunnelProperties(regionEnforcementAndTunnelProperties::setProperty, sshTunnelProperties));

        // Do not set TLS through `withTlsEnabled` for secure connection(which creates an SSLSocket) in `CatalogNetworkMonitorProperties` in as layered sockets are created from plain socket before SSL handshake https://github.com/apache/httpcomponents-client/blob/master/httpclient5/src/main/java/org/apache/hc/client5/http/ssl/SSLConnectionSocketFactory.java#L255-L286
        addCatalogNetworkMonitorProperties(regionEnforcementAndTunnelProperties::setProperty, CatalogNetworkMonitorProperties.generateFrom(crossRegionConfig, catalogHandle));

        properties.setProperty(CUSTOM_SOCKET_FACTORY.getKey(), GalaxyClickHouseSocketFactory.class.getName());
        properties.setProperty(CUSTOM_SOCKET_FACTORY_OPTIONS.getKey(), serializeProperties(catalogHandle.getId(), regionEnforcementAndTunnelProperties));

        // The connector expects byte array for FixedString and String types
        properties.setProperty(USE_BINARY_STRING.getKey(), "true");
        return new ClickHouseConnectionFactory(DriverConnectionFactory.builder(new ClickHouseDriver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(properties)
                .setOpenTelemetry(openTelemetry)
                .build());
    }
}
