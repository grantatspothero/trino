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
import com.clickhouse.client.internal.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import com.clickhouse.client.internal.apache.hc.core5.http.protocol.HttpContext;
import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;

import java.net.Socket;
import java.util.Map;
import java.util.Properties;

import static io.trino.plugin.clickhouse.galaxy.GalaxyClickhouseUtils.deserializeProperties;
import static java.util.Objects.requireNonNull;

public class GalaxyPlainConnectionSocketFactory
        extends PlainConnectionSocketFactory
{
    private final GalaxySqlSocketFactory galaxySqlSocketFactory;

    public GalaxyPlainConnectionSocketFactory(ClickHouseConfig config)
    {
        Map<String, String> socketFactoryOptions = config.getCustomSocketFactoryOptions();
        Properties properties = deserializeProperties(socketFactoryOptions);
        requireNonNull(properties, "properties is null");
        galaxySqlSocketFactory = new GalaxySqlSocketFactory(properties);
    }

    @Override
    public Socket createSocket(HttpContext context)
    {
        return galaxySqlSocketFactory.createSocket();
    }
}
