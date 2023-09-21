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
package io.trino.plugin.objectstore.scheduler;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.starburst.stargate.accountwork.client.HttpTrinoMaterializedViewScheduler;
import io.starburst.stargate.accountwork.client.TrinoMaterializedViewScheduler;
import io.starburst.stargate.id.ClusterId;
import io.trino.plugin.iceberg.WorkScheduler;
import io.trino.plugin.objectstore.ForGalaxyAccountWork;
import io.trino.plugin.objectstore.GalaxyAccountWorkConfig;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GalaxyWorkSchedulerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GalaxyAccountWorkConfig.class);
        newOptionalBinder(binder, WorkScheduler.class).setBinding().to(GalaxyWorkScheduler.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("galaxy-account-work", ForGalaxyAccountWork.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });
    }

    @Provides
    @Singleton
    public static TrinoMaterializedViewScheduler createTrinoMaterializedViewScheduler(@ForGalaxyAccountWork HttpClient httpClient, GalaxyAccountWorkConfig config)
    {
        return new HttpTrinoMaterializedViewScheduler(config.getAccountUri(), httpClient, new ClusterId(config.getClusterId()));
    }
}
