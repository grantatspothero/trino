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
package io.trino.plugin.postgresql.galaxy;

import org.postgresql.ssl.LibPQFactory;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;

public class GalaxyPostgreSqlSslSocketFactory
        extends LibPQFactory
{
    public GalaxyPostgreSqlSslSocketFactory(Properties properties)
            throws PSQLException
    {
        super(properties);
    }

    @Override
    public Socket createSocket()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Socket createSocket(InetAddress host, int port)
    {
        throw new UnsupportedOperationException();
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
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
    {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("RedundantMethodOverride")
    @Override
    public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
            throws IOException
    {
        // The PostgreSQL driver (as of 42.2.10) only creates an SSL socket from another Socket created by the standard socket factory.
        // We want an error to be thrown if this assumption is ever violated (when non-wrapping createSocket() methods are called).
        return super.createSocket(socket, host, port, autoClose);
    }

    @SuppressWarnings("RedundantMethodOverride")
    @Override
    public Socket createSocket(Socket socket, InputStream consumed, boolean autoClose)
            throws IOException
    {
        // The PostgreSQL driver (as of 42.2.10) only creates an SSL socket from another Socket created by the standard socket factory.
        // We want an error to be thrown if this assumption is ever violated (when non-wrapping createSocket() methods are called).
        return super.createSocket(socket, consumed, autoClose);
    }
}
