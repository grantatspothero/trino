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
import com.clickhouse.client.ClickHouseSocketFactory;
import com.clickhouse.client.internal.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import com.clickhouse.client.internal.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;

import java.io.IOException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class GalaxyClickHouseSocketFactory
        implements ClickHouseSocketFactory
{
    @Override
    public <T> T create(ClickHouseConfig config, Class<T> clazz)
            throws IOException, UnsupportedOperationException
    {
        requireNonNull(config, "config is null");
        requireNonNull(clazz, "clazz is null");
        if (SSLConnectionSocketFactory.class.equals(clazz)) {
            return clazz.cast(new GalaxySSLConnectionSocketFactory(config));
        }
        else if (PlainConnectionSocketFactory.class.equals(clazz)) {
            return clazz.cast(new GalaxyPlainConnectionSocketFactory(config));
        }

        throw new UnsupportedOperationException(format("Class %s is not supported", clazz));
    }

    @Override
    public boolean supports(Class<?> clazz)
    {
        return PlainConnectionSocketFactory.class.equals(clazz) || SSLConnectionSocketFactory.class.equals(clazz);
    }
}
