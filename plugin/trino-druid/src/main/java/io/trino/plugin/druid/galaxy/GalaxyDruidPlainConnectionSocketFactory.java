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

import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.core5.http.protocol.HttpContext;

import java.net.Socket;
import java.util.Properties;

public class GalaxyDruidPlainConnectionSocketFactory
        extends PlainConnectionSocketFactory
{
    private final GalaxySqlSocketFactory socketFactory;

    public GalaxyDruidPlainConnectionSocketFactory(Properties properties)
    {
        this.socketFactory = new GalaxySqlSocketFactory(properties);
    }

    @Override
    public Socket createSocket(HttpContext context)
    {
        return socketFactory.createSocket();
    }
}
