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
package io.trino.plugin.druid.galaxy;

import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.remote.AvaticaCommonsHttpClientImpl;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.AvaticaHttpClientFactoryImpl;
import org.apache.calcite.avatica.remote.KerberosConnection;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;

import java.net.URL;
import java.util.Properties;

import static io.trino.plugin.druid.galaxy.GalaxyCommonsHttpClientPoolCache.getPool;
import static java.util.Objects.requireNonNull;

/**
 * Extend default implementation so that we can override the socket in Galaxy.
 * Changes include:
 * Remove config controlled basic and krb authentication code since its not valid for Galaxy
 * Always use default AvaticaCommonsHttpClientImpl as the client
 */
public class GalaxyAvaticaHttpClientFactoryImpl
        extends AvaticaHttpClientFactoryImpl
{
    private final Properties galaxyProperties;

    public GalaxyAvaticaHttpClientFactoryImpl(Properties galaxyProperties)
    {
        this.galaxyProperties = requireNonNull(galaxyProperties, "galaxyProperties is null");
    }

    @Override
    public AvaticaHttpClient getClient(URL url, ConnectionConfig ignoredConfig, KerberosConnection ignoredConnection)
    {
        AvaticaCommonsHttpClientImpl client = new AvaticaCommonsHttpClientImpl(url);
        PoolingHttpClientConnectionManager poolingConnectionManager = getPool(galaxyProperties);
        client.setHttpClientPool(poolingConnectionManager);
        return client;
    }
}
