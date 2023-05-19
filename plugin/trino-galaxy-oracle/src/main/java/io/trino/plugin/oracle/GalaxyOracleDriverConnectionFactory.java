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
import io.trino.plugin.base.galaxy.RegionVerifier;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.sshtunnel.SshTunnelManager;
import io.trino.sshtunnel.SshTunnelManager.Tunnel;
import io.trino.sshtunnel.SshTunnelPropertiesMapper;
import oracle.jdbc.driver.OracleDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.oracle.OracleConnectionUrlFormatUtils.buildSshTunnelConnectionUrl;
import static io.trino.plugin.oracle.OracleConnectionUrlFormatUtils.getHostPort;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class GalaxyOracleDriverConnectionFactory
        implements ConnectionFactory
{
    private final OracleDriver driver;
    private final Properties connectionProperties;
    private final CredentialPropertiesProvider<String, String> credentialPropertiesProvider;
    private final Optional<SshTunnelManager> tunnelManager;
    private RegionVerifier regionVerifier;
    private HostAndPort databaseHostAndPort;
    private String connectionUrl;

    public GalaxyOracleDriverConnectionFactory(OracleDriver driver, String connectionUrl, Properties connectionProperties, CredentialProvider credentialProvider)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
        this.credentialPropertiesProvider = new DefaultCredentialPropertiesProvider(requireNonNull(credentialProvider, "credentialProvider is null"));
        this.tunnelManager =
                SshTunnelPropertiesMapper
                        .getSshTunnelProperties(name -> Optional.ofNullable(connectionProperties.getProperty(name)))
                        .map(SshTunnelManager::getCached);
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Properties properties = getCredentialProperties(session.getIdentity());

        if (isNull(databaseHostAndPort)) {
            databaseHostAndPort = getHostPort(this.connectionUrl);
        }
        if (isNull(regionVerifier)) {
            regionVerifier = new RegionVerifier(connectionProperties);
        }
        String host = tunnelManager.map(sshTunnelManager -> sshTunnelManager.getSshServer().getHost()).orElseGet(databaseHostAndPort::getHost);
        regionVerifier.verifyLocalRegion(tunnelManager.isPresent() ? "SSH tunnel server" : "Database server", host);

        if (tunnelManager.isPresent()) {
            Tunnel tunnel = tunnelManager.get().getOrCreateTunnel(databaseHostAndPort);
            connectionUrl = buildSshTunnelConnectionUrl(connectionUrl, tunnel.getLocalTunnelPort());
        }
        Connection connection = driver.connect(connectionUrl, properties);
        checkState(connection != null, "Oracle driver returned null connection, make sure the connection URL '%s' is valid for the driver %s", connection, driver);
        return connection;
    }

    private Properties getCredentialProperties(ConnectorIdentity identity)
    {
        Properties properties = new Properties();
        properties.putAll(connectionProperties);
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        return properties;
    }
}
