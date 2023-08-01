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
package io.trino.connector.informationschema.galaxy;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.connector.informationschema.InformationSchemaPageSourceProvider;
import io.trino.connector.system.TableCommentSystemTable;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GalaxyCacheModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        Multibinder<SystemTable> globalTableBinder = Multibinder.newSetBinder(binder, SystemTable.class);

        GalaxyCacheConfig galaxyCacheConfig = buildConfigObject(GalaxyCacheConfig.class);
        if (galaxyCacheConfig.getEnabled()) {
            binder.bind(InformationSchemaPageSourceProvider.class).to(GalaxyCacheInformationSchemaPageSourceProvider.class).in(Scopes.SINGLETON);

            httpClientBinder(binder).bindHttpClient("galaxy-cache", ForGalaxyCache.class)
                    .withConfigDefaults(config -> {
                        config.setIdleTimeout(new Duration(30, SECONDS));
                        config.setRequestTimeout(new Duration(10, SECONDS));
                    });

            configBinder(binder).bindConfig(GalaxyCacheConfig.class);
            binder.bind(GalaxyCacheClient.class).in(Scopes.SINGLETON);
            binder.bind(GalaxyCacheStats.class).in(Scopes.SINGLETON);

            globalTableBinder.addBinding().to(GalaxyCacheTableCommentSystemTable.class).in(Scopes.SINGLETON);
            globalTableBinder.addBinding().to(GalaxyCacheStatusSystemTable.class).in(Scopes.SINGLETON);
            globalTableBinder.addBinding().to(GalaxyCacheStatsSystemTable.class).in(Scopes.SINGLETON);

            newSetBinder(binder, SystemSessionPropertiesProvider.class).addBinding().to(GalaxyCacheSessionProperties.class);

            binder.bind(GalaxyCacheRefreshCacheProcedure.class).in(Scopes.SINGLETON);

            Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
            procedures.addBinding().toProvider(GalaxyCacheRefreshCacheProcedure.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(InformationSchemaPageSourceProvider.class).in(Scopes.SINGLETON);
            globalTableBinder.addBinding().to(TableCommentSystemTable.class).in(Scopes.SINGLETON);
        }
    }
}
