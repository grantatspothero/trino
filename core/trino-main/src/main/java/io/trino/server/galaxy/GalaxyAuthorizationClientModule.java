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
package io.trino.server.galaxy;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.starburst.stargate.accesscontrol.client.HttpTrinoSecurityClient;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.trino.server.security.galaxy.CatalogIds;
import io.trino.server.security.galaxy.ForGalaxySystemAccessControl;
import io.trino.server.security.galaxy.GalaxyAccessControlConfig;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GalaxyAuthorizationClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(CatalogIds.class).in(SINGLETON);
        configBinder(binder).bindConfig(GalaxyAccessControlConfig.class);

        bindHttpClient(binder);
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
                });
    }

    @Provides
    @Singleton
    public static TrinoSecurityApi createTrinoSecurityApi(@ForGalaxySystemAccessControl HttpClient httpClient, GalaxyAccessControlConfig config)
    {
        return new HttpTrinoSecurityClient(requireNonNull(config, "config is null").getAccountUri(), httpClient);
    }
}
