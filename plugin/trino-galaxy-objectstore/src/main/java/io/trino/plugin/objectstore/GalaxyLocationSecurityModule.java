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
import com.google.inject.Singleton;
import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.starburst.stargate.accesscontrol.client.HttpTrinoLocationClient;
import io.starburst.stargate.accesscontrol.client.TrinoLocationApi;
import io.trino.plugin.hive.LocationAccessControl;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GalaxyLocationSecurityModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newOptionalBinder(binder, LocationAccessControl.class)
                .setBinding().to(GalaxyLocationAccessControl.class);

        configBinder(binder).bindConfig(GalaxyLocationSecurityConfig.class);

        httpClientBinder(binder).bindHttpClient("galaxy-location-security", ForGalaxyLocationSecurity.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });
    }

    @Provides
    @Singleton
    public static TrinoLocationApi createTrinoSecurityApi(@ForGalaxyLocationSecurity HttpClient httpClient, GalaxyLocationSecurityConfig config)
    {
        return new HttpTrinoLocationClient(config.getAccountUri(), httpClient);
    }
}
