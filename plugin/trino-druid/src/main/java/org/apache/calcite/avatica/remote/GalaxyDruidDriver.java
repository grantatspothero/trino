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
package org.apache.calcite.avatica.remote;

import io.trino.plugin.druid.galaxy.GalaxyAvaticaHttpClientFactoryImpl;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Inlined Driver for Galaxy. This is added to support Imply which does not allow any custom property in the connection parameters.
 */
public class GalaxyDruidDriver
        extends UnregisteredDriver
{
    private final Properties galaxyProperties;

    public static final String CONNECT_STRING_PREFIX = "jdbc:avatica:remote:";

    public GalaxyDruidDriver(Properties galaxyProperties)
    {
        super();
        this.galaxyProperties = requireNonNull(galaxyProperties, "galaxyProperties is null");
    }

    @Override
    protected String getConnectStringPrefix()
    {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    protected DriverVersion createDriverVersion()
    {
        return DriverVersion.load(
                GalaxyDruidDriver.class,
                "org-apache-calcite-jdbc.properties",
                "Avatica Remote JDBC Driver",
                "1.22.0-galaxy", // modified for Galaxy verifier to return a meaningful version
                "Avatica",
                "1.22.0");
    }

    @Override
    protected Collection<ConnectionProperty> getConnectionProperties()
    {
        List<ConnectionProperty> list = new ArrayList<>();
        Collections.addAll(list, BuiltInConnectionProperty.values());
        Collections.addAll(list, AvaticaRemoteConnectionProperty.values());
        return list;
    }

    @Override
    public Meta createMeta(AvaticaConnection connection)
    {
        ConnectionConfig config = connection.config();
        // Create a single Service and set it on the Connection instance
        Service service = createService(config);
        connection.setService(service);
        return new RemoteMeta(connection, service);
    }

    /**
     * Modifies the default implementation to return RemoteService always.
     * We do not need to override factory or serialization in Trino
     */
    private Service createService(ConnectionConfig config)
    {
        checkState(config.url() != null, "connection url cannot be null");
        AvaticaHttpClient httpClient = getHttpClient(config.url());
        return new RemoteService(httpClient);
    }

    /**
     * Always use GalaxyAvaticaHttpClientFactoryImpl to get the AvaticaHttpClient
     */
    private AvaticaHttpClient getHttpClient(String connectionUrl)
    {
        URL url;
        try {
            url = new URL(connectionUrl);
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        GalaxyAvaticaHttpClientFactoryImpl httpClientFactory = new GalaxyAvaticaHttpClientFactoryImpl(galaxyProperties);
        return httpClientFactory.getClient(url, null, null);
    }

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        AvaticaConnection connection = (AvaticaConnection) super.connect(url, info);

        if (connection == null) {
            // It's not an url for our driver
            return null;
        }

        Service service = connection.getService();
        // super.connect(...) should be creating a service and setting it in the AvaticaConnection
        checkState(service != null, "service cannot be null");

        service.apply(
                new Service.OpenConnectionRequest(connection.id,
                        Service.OpenConnectionRequest.serializeProperties(info))); // Don't pass galaxy specific properties because Imply does not like having external connection properties

        return connection;
    }
}
