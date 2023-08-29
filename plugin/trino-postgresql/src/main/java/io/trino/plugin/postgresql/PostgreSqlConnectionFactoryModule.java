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
package io.trino.plugin.postgresql;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.DataSize;
import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import io.trino.plugin.base.galaxy.RegionEnforcementConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.postgresql.galaxy.GalaxyPostgreSqlSslSocketFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;
import org.postgresql.Driver;

import java.util.Properties;

import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogId;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogName;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCrossRegionReadLimit;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCrossRegionWriteLimit;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;
import static org.postgresql.PGProperty.REWRITE_BATCHED_INSERTS;

public class PostgreSqlConnectionFactoryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder) {}

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            RegionEnforcementConfig regionEnforcementConfig,
            SshTunnelConfig sshTunnelConfig,
            CredentialProvider credentialProvider)
    {
        Properties connectionProperties = new Properties();
        connectionProperties.put(REWRITE_BATCHED_INSERTS.getName(), "true");

        connectionProperties.put("socketFactory", GalaxySqlSocketFactory.class.getName());
        connectionProperties.put("sslfactory", GalaxyPostgreSqlSslSocketFactory.class.getName());
        addCatalogName(connectionProperties, catalogHandle.getCatalogName());
        addCatalogId(connectionProperties, catalogHandle.getVersion().toString());
        RegionVerifierProperties.addRegionVerifierProperties(connectionProperties::setProperty, RegionVerifierProperties.generateFrom(regionEnforcementConfig));
        if (regionEnforcementConfig.getAllowCrossRegionAccess()) {
            DataSize crossRegionReadLimit = regionEnforcementConfig.getCrossRegionReadLimit();
            addCrossRegionReadLimit(connectionProperties, crossRegionReadLimit);
            DataSize crossRegionWriteLimit = regionEnforcementConfig.getCrossRegionWriteLimit();
            addCrossRegionWriteLimit(connectionProperties, crossRegionWriteLimit);
        }

        SshTunnelProperties.generateFrom(sshTunnelConfig)
                .ifPresent(sshTunnelProperties -> addSshTunnelProperties(connectionProperties::setProperty, sshTunnelProperties));

        return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), connectionProperties, credentialProvider);
    }
}
