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

import io.airlift.log.Logger;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;

import javax.net.ssl.HostnameVerifier;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.CATALOG_ID_PROPERTY_NAME;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.TLS_ENABLED_PROPERTY_NAME;
import static java.util.Objects.requireNonNull;

/**
 * Creates and returns a PoolingHttpClientConnectionManager object.
 * Copied from Avatica's CommonsHttpClientPoolCache, but modified for adding custom ConnectionSocketFactory.
 * Cache the catalog specific pool.
 */
public class GalaxyCommonsHttpClientPoolCache
{
    private static final int MAX_POOLED_CONNECTION_PER_ROUTE_DEFAULT = 25;
    private static final int MAX_POOLED_CONNECTIONS_DEFAULT = 100;
    private static final ConcurrentHashMap<String, PoolingHttpClientConnectionManager> CACHED_POOLS =
            new ConcurrentHashMap<>();

    private static final Logger LOG = Logger.get(GalaxyCommonsHttpClientPoolCache.class);

    private GalaxyCommonsHttpClientPoolCache()
    {
        //do not instantiate
    }

    public static PoolingHttpClientConnectionManager getPool(Properties galaxyProperties)
    {
        return CACHED_POOLS.computeIfAbsent(
                requireNonNull(galaxyProperties.getProperty(CATALOG_ID_PROPERTY_NAME)),
                k -> setupPool(galaxyProperties));
    }

    private static PoolingHttpClientConnectionManager setupPool(Properties galaxyProperties)
    {
        Registry<ConnectionSocketFactory> registry = createConnectionSocketFactoryRegistry(galaxyProperties);
        PoolingHttpClientConnectionManager poolingConnectionManager = new PoolingHttpClientConnectionManager(registry);
        poolingConnectionManager.setMaxTotal(MAX_POOLED_CONNECTIONS_DEFAULT);
        poolingConnectionManager.setDefaultMaxPerRoute(MAX_POOLED_CONNECTION_PER_ROUTE_DEFAULT);
        LOG.debug("Created new poolingConnectionManager {%s}", poolingConnectionManager);
        return poolingConnectionManager;
    }

    private static Registry<ConnectionSocketFactory> createConnectionSocketFactoryRegistry(Properties galaxyProperties)
    {
        RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
        if (galaxyProperties.containsKey(TLS_ENABLED_PROPERTY_NAME)) {
            configureHttpsRegistry(registryBuilder, galaxyProperties);
        }
        else {
            configureHttpRegistry(registryBuilder, galaxyProperties);
        }

        return registryBuilder.build();
    }

    private static void configureHttpsRegistry(
            RegistryBuilder<ConnectionSocketFactory> registryBuilder,
            Properties galaxyProperties)
    {
        try {
            // Modified for Galaxy to use NOOP instance always
            HostnameVerifier verifier = NoopHostnameVerifier.INSTANCE;
            GalaxyDruidSSLConnectionSocketFactory sslFactory = new GalaxyDruidSSLConnectionSocketFactory(
                    new GalaxyDruidSSLSocketFactory(galaxyProperties), verifier);
            registryBuilder.register("https", sslFactory);
        }
        catch (Exception e) {
            throw new RuntimeException("HTTPS registry configuration failed", e);
        }
    }

    private static void configureHttpRegistry(RegistryBuilder<ConnectionSocketFactory> registryBuilder, Properties properties)
    {
        registryBuilder.register("http", new GalaxyDruidPlainConnectionSocketFactory(properties));
    }
}
