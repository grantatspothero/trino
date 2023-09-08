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
package io.trino.plugin.clickhouse.galaxy;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseSslContextProvider;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.client.internal.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import com.clickhouse.client.internal.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import com.clickhouse.client.internal.apache.hc.core5.http.protocol.HttpContext;
import com.clickhouse.client.internal.apache.hc.core5.ssl.SSLContexts;
import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import java.net.Socket;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.clickhouse.galaxy.GalaxyClickhouseUtils.deserializeProperties;

public class GalaxySSLConnectionSocketFactory
        extends SSLConnectionSocketFactory
{
    private final GalaxySqlSocketFactory galaxySqlSocketFactory;

    public GalaxySSLConnectionSocketFactory(ClickHouseConfig config)
            throws SSLException
    {
        super(ClickHouseSslContextProvider.getProvider().getSslContext(SSLContext.class, config)
                        .orElse(SSLContexts.createDefault()),
                config.getSslMode() == ClickHouseSslMode.STRICT
                        ? new DefaultHostnameVerifier()
                        : (hostname, session) -> true);

        Map<String, String> socketFactoryOptions = config.getCustomSocketFactoryOptions();
        checkArgument(!socketFactoryOptions.isEmpty(), "socketFactoryOptions is empty");
        Properties properties = deserializeProperties(socketFactoryOptions);
        galaxySqlSocketFactory = new GalaxySqlSocketFactory(properties);
    }

    @Override
    public Socket createSocket(HttpContext httpContext)
    {
        return galaxySqlSocketFactory.createSocket();
    }
}
