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
package io.trino.plugin.mariadb.galaxy;

import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.util.ConfigurableSocketFactory;

import java.net.InetAddress;
import java.net.Socket;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class GalaxyMariaDbSocketFactory
        extends ConfigurableSocketFactory
{
    private GalaxySqlSocketFactory socketFactory;

    @Override
    public void setConfiguration(Configuration conf, String host)
    {
        if (isNull(socketFactory)) {
            socketFactory = new GalaxySqlSocketFactory(conf.nonMappedOptions());
        }
    }

    @Override
    public Socket createSocket()
    {
        requireNonNull(socketFactory, "socketFactory is null");
        return socketFactory.createSocket();
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
