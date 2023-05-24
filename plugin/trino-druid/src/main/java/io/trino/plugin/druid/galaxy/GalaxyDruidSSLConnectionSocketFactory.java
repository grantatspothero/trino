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

import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.protocol.HttpContext;

import javax.net.ssl.HostnameVerifier;

import java.net.Socket;

import static java.util.Objects.requireNonNull;

public class GalaxyDruidSSLConnectionSocketFactory
        extends SSLConnectionSocketFactory
{
    private final GalaxyDruidSSLSocketFactory galaxySslSocketFactory;

    public GalaxyDruidSSLConnectionSocketFactory(
            GalaxyDruidSSLSocketFactory galaxySslSocketFactory,
            HostnameVerifier hostnameVerifier)
    {
        super(
                requireNonNull(galaxySslSocketFactory, "galaxySslSocketFactory is null"),
                null,
                null,
                requireNonNull(hostnameVerifier, "hostnameVerifier is null"));
        this.galaxySslSocketFactory = galaxySslSocketFactory;
    }

    @Override
    public Socket createSocket(HttpContext context)
    {
        return galaxySslSocketFactory.createSocket();
    }
}
