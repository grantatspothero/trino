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
package io.trino.server.security.galaxy;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.starburst.stargate.accesscontrol.client.HttpTrinoSecurityClient;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.trino.connector.CatalogManagerConfig;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.server.galaxy.GalaxyConfig;
import io.trino.server.galaxy.catalogs.CatalogResolver;
import io.trino.server.galaxy.catalogs.LiveCatalogsTransactionManager;

import java.net.URI;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.trino.connector.CatalogManagerConfig.CatalogMangerKind.LIVE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GalaxySecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GalaxyConfig.class);
        newOptionalBinder(binder, SystemSecurityMetadata.class).setBinding().to(GalaxySecurityMetadata.class).in(SINGLETON);
        binder.bind(GalaxySecurityMetadata.class).in(SINGLETON);

        CatalogManagerConfig.CatalogMangerKind kind = buildConfigObject(CatalogManagerConfig.class).getCatalogMangerKind();
        if (kind == LIVE) {
            binder.bind(LiveCatalogsTransactionManager.class).in(SINGLETON);
            binder.bind(CatalogResolver.class).to(LiveCatalogsTransactionManager.class).in(SINGLETON);
        }
        else {
            binder.bind(StaticCatalogResolver.class).in(SINGLETON);
            binder.bind(CatalogResolver.class).to(StaticCatalogResolver.class).in(SINGLETON);
        }
        configBinder(binder).bindConfig(GalaxyAccessControlConfig.class);

        bindHttpClient(binder);

        install(new GalaxySystemAccessModule());
    }

    public static void bindHttpClient(Binder binder)
    {
        httpClientBinder(binder).bindHttpClient("galaxy-access-control", ForGalaxySystemAccessControl.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(20, SECONDS));
                    // Automatic https is disabled because this client is for external
                    // communication back to the Galaxy portal.
                    config.setAutomaticHttpsSharedSecret(null);
                    // Using 3 times the default, as domain is always the same, so all connections hit same server
                    // previously used default (20) was throttling requests, doing 3x20 should serve as new conservative default
                    // this is subject to increase if we see throttling still, or decrease if we overwhelm RBAC
                    config.setMaxConnectionsPerServer(60);
                });
    }

    @Provides
    @Singleton
    public static TrinoSecurityApi createTrinoSecurityApi(@ForGalaxySystemAccessControl HttpClient httpClient, GalaxyAccessControlConfig config)
    {
        requireNonNull(config, "config is null");
        URI uri = config.getAccessControlOverrideUri().orElse(config.getAccountUri());
        return new HttpTrinoSecurityClient(uri, httpClient);
    }
}
