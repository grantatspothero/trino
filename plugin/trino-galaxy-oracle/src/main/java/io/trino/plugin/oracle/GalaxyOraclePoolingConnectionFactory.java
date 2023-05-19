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

import com.google.common.net.HostAndPort;
import com.starburstdata.trino.plugins.oracle.OraclePoolingConnectionFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.galaxy.RegionVerifier;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.sshtunnel.SshTunnelManager;
import io.trino.sshtunnel.SshTunnelManager.Tunnel;
import io.trino.sshtunnel.SshTunnelPropertiesMapper;
import oracle.ucp.UniversalConnectionPoolException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static io.trino.plugin.oracle.OracleConnectionUrlFormatUtils.buildSshTunnelConnectionUrl;
import static io.trino.plugin.oracle.OracleConnectionUrlFormatUtils.getHostPort;
import static java.util.Objects.isNull;

public class GalaxyOraclePoolingConnectionFactory
        extends OraclePoolingConnectionFactory
{
    private final Optional<SshTunnelManager> tunnelManager;
    private final String connectionUrl;
    private final Object updateSshTunnelUrlLock = new Object();
    private final Properties connectionProperties;
    private HostAndPort databaseHostAndPort;
    private RegionVerifier regionVerifier;
    private int sshTunnelPort;
    private boolean isUsingLatestSshTunnel;

    public GalaxyOraclePoolingConnectionFactory(CatalogName catalogName, BaseJdbcConfig config, Properties connectionProperties, Optional<CredentialProvider> credentialProvider, OracleConfig oracleConfig)
    {
        super(catalogName, config, connectionProperties, credentialProvider, oracleConfig);
        this.connectionProperties = connectionProperties;
        this.connectionUrl = config.getConnectionUrl();
        this.tunnelManager =
                SshTunnelPropertiesMapper
                        .getSshTunnelProperties(name -> Optional.ofNullable(connectionProperties.getProperty(name)))
                        .map(SshTunnelManager::getCached);
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        if (isNull(databaseHostAndPort)) {
            databaseHostAndPort = getHostPort(this.connectionUrl);
            sshTunnelPort = this.databaseHostAndPort.getPort();
        }
        if (isNull(regionVerifier)) {
            regionVerifier = new RegionVerifier(connectionProperties);
        }
        regionVerifier.verifyLocalRegion(
                tunnelManager.isPresent() ? "SSH tunnel server" : "Database server",
                tunnelManager.map(sshTunnelManager -> sshTunnelManager.getSshServer().getHost()).orElseGet(databaseHostAndPort::getHost));
        if (tunnelManager.isPresent()) {
            recreateDatasourceWithSshTunnel();
        }
        return super.openConnection(session);
    }

    private void recreateDatasourceWithSshTunnel()
            throws SQLException
    {
        Tunnel tunnel = tunnelManager.map(sshTunnelManager -> sshTunnelManager.getOrCreateTunnel(databaseHostAndPort)).orElseThrow();
        int currentSshTunnelPort = tunnel.getLocalTunnelPort();
        if (currentSshTunnelPort != sshTunnelPort) {
            synchronized (updateSshTunnelUrlLock) {
                if (!isUsingLatestSshTunnel) {
                    isUsingLatestSshTunnel = true;
                    try {
                        super.close(); // destroy the current dataSource
                        // Create a new dataSource with updated connection url created from new ssh tunnel
                        createDatasource(buildSshTunnelConnectionUrl(connectionUrl, currentSshTunnelPort));
                        sshTunnelPort = currentSshTunnelPort;
                    }
                    catch (UniversalConnectionPoolException e) {
                        throw new RuntimeException(e);
                    }
                    finally {
                        // Added in finally and not in try block so that code failures in try block
                        // will still allow another thread to recreate the dataSource.
                        isUsingLatestSshTunnel = false;
                    }
                }
            }
        }
    }
}
