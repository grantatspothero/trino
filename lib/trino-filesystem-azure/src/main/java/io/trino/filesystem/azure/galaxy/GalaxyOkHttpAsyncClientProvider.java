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
package io.trino.filesystem.azure.galaxy;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpClientProvider;
import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.core.util.Configuration;
import com.azure.core.util.ConfigurationProperty;
import com.azure.core.util.ConfigurationPropertyBuilder;
import com.azure.core.util.HttpClientOptions;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import okhttp3.ConnectionPool;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/*
 * This class is meant to be similar to OkHttpAsyncClientProvider. The only difference is that a custom
 * network interceptor used for Galaxy is added in order to monitor and record the network usage.
 */
public class GalaxyOkHttpAsyncClientProvider
        implements HttpClientProvider
{
    public GalaxyOkHttpAsyncClientProvider()
    {
    }

    @Override
    public HttpClient createInstance()
    {
        return new OkHttpAsyncHttpClientBuilder()
                .build();
    }

    @Override
    public HttpClient createInstance(HttpClientOptions httpClientOptions)
    {
        if (httpClientOptions == null) {
            return createInstance();
        }
        Configuration configuration = requireNonNull(httpClientOptions.getConfiguration(), "configuration is null");
        RegionVerifierProperties regionVerifierProperties = RegionVerifierProperties.getRegionVerifierProperties(
                (propertyName) -> getPropertyValueFromConfiguration(configuration, propertyName));
        CatalogNetworkMonitorProperties catalogNetworkMonitorProperties = CatalogNetworkMonitorProperties.getCatalogNetworkMonitorProperties(
                (propertyName) -> Optional.ofNullable(getPropertyValueFromConfiguration(configuration, propertyName)));

        GalaxyNetworkInterceptor galaxyNetworkInterceptor = new GalaxyNetworkInterceptor(regionVerifierProperties, catalogNetworkMonitorProperties);

        Integer poolSize = httpClientOptions.getMaximumConnectionPoolSize();
        int maximumConnectionPoolSize = (poolSize != null && poolSize > 0)
                ? poolSize
                : 5; // By default, OkHttp uses a maximum idle connection count of 5.

        ConnectionPool connectionPool = new ConnectionPool(
                maximumConnectionPoolSize,
                httpClientOptions.getConnectionIdleTimeout().toMillis(),
                TimeUnit.MILLISECONDS);

        return new OkHttpAsyncHttpClientBuilder()
                .proxy(httpClientOptions.getProxyOptions())
                .configuration(httpClientOptions.getConfiguration())
                .connectionTimeout(httpClientOptions.getConnectTimeout())
                .writeTimeout(httpClientOptions.getWriteTimeout())
                .readTimeout(httpClientOptions.getReadTimeout())
                .addNetworkInterceptor(galaxyNetworkInterceptor)
                .connectionPool(connectionPool)
                .build();
    }

    private String getPropertyValueFromConfiguration(Configuration configuration, String propertyName)
    {
        return configuration.get(createConfigurationProperty(propertyName));
    }

    private ConfigurationProperty<String> createConfigurationProperty(String propertyName)
    {
        return ConfigurationPropertyBuilder.ofString(propertyName).build();
    }
}
