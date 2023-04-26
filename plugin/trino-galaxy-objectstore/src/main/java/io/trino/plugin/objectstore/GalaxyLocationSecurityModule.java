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
package io.trino.plugin.objectstore;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.starburst.stargate.accesscontrol.client.HttpTrinoLocationClient;
import io.starburst.stargate.accesscontrol.client.HttpTrinoSecurityClient;
import io.starburst.stargate.accesscontrol.client.TrinoLocationApi;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.trino.plugin.hive.LocationAccessControl;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GalaxyLocationSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        if (!buildConfigObject(GalaxySecurityConfig.class).isEnabled()) {
            install(new DisabledModule());
        }
        else {
            install(new EnabledModule());
        }
    }

    public static class DisabledModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            newOptionalBinder(binder, LocationAccessControl.class)
                    .setBinding().toInstance(LocationAccessControl.ALLOW_ALL);
            newOptionalBinder(binder, TrinoSecurityControl.class)
                    .setBinding().toInstance(TrinoSecurityControl.ALLOW_ALL);
        }
    }

    public static class EnabledModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            newOptionalBinder(binder, LocationAccessControl.class)
                    .setBinding().to(GalaxyLocationAccessControl.class).in(Scopes.SINGLETON);
            newOptionalBinder(binder, TrinoSecurityControl.class)
                    .setBinding().to(GalaxyTrinoSecurityControl.class).in(Scopes.SINGLETON);

            configBinder(binder).bindConfig(GalaxySecurityConfig.class);

            httpClientBinder(binder).bindHttpClient("galaxy-location-security", ForGalaxyLocationSecurity.class)
                    .withConfigDefaults(config -> {
                        config.setIdleTimeout(new Duration(30, SECONDS));
                        config.setRequestTimeout(new Duration(10, SECONDS));
                    });
        }

        @Provides
        @Singleton
        public static TrinoLocationApi createTrinoLocationApi(@ForGalaxyLocationSecurity HttpClient httpClient, GalaxySecurityConfig config)
        {
            return new HttpTrinoLocationClient(config.getAccountUri(), httpClient);
        }

        @Provides
        @Singleton
        public static TrinoSecurityApi createTrinoSecurityApi(@ForGalaxyLocationSecurity HttpClient httpClient, GalaxySecurityConfig config)
        {
            return new HttpTrinoSecurityClient(config.getAccountUri(), httpClient);
        }
    }
}
