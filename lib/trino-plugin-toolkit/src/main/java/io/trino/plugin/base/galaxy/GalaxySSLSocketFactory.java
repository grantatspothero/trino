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
package io.trino.plugin.base.galaxy;

import javax.net.ssl.SSLSocketFactory;

import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;

public class GalaxySSLSocketFactory
        extends SSLSocketFactory
{
    private final GalaxySqlSocketFactory socketFactory;

    public GalaxySSLSocketFactory(RegionVerifierProperties regionVerifierProperties, CatalogNetworkMonitorProperties catalogNetworkMonitorProperties)
    {
        Properties connectionProperties = new Properties();
        RegionVerifierProperties.addRegionVerifierProperties(connectionProperties::setProperty, regionVerifierProperties);
        CatalogNetworkMonitorProperties.addCatalogNetworkMonitorProperties(connectionProperties::setProperty, catalogNetworkMonitorProperties);
        this.socketFactory = new GalaxySqlSocketFactory(connectionProperties);
    }

    @Override
    public String[] getDefaultCipherSuites()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getSupportedCipherSuites()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket()
    {
        return socketFactory.createSocket();
    }

    @Override
    public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
    {
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress host, int port)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
    {
        throw new UnsupportedOperationException();
    }
}
