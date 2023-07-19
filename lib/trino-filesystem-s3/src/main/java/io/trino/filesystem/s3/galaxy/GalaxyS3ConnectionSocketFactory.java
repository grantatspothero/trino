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
package io.trino.filesystem.s3.galaxy;

import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import org.apache.http.HttpHost;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class GalaxyS3ConnectionSocketFactory
        implements ConnectionSocketFactory
{
    private final GalaxySqlSocketFactory galaxySqlSocketFactory;

    public GalaxyS3ConnectionSocketFactory(Properties properties)
    {
        this.galaxySqlSocketFactory = new GalaxySqlSocketFactory(properties);
    }

    @Override
    public Socket createSocket(HttpContext httpContext)
    {
        return galaxySqlSocketFactory.createSocket();
    }

    @Override
    public Socket connectSocket(
            int connectTimeout,
            Socket socket,
            HttpHost host,
            InetSocketAddress remoteAddress,
            InetSocketAddress localAddress,
            HttpContext httpContext)
                throws IOException
    {
        requireNonNull(remoteAddress, "remoteAddress is null");
        Socket sock = socket != null ? socket : createSocket(httpContext);

        if (localAddress != null) {
            sock.bind(localAddress);
        }

        sock.connect(remoteAddress, connectTimeout);

        return sock;
    }
}
