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
package io.trino.plugin.iceberg.catalog.meteor;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class MeteorCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(MeteorCatalogConfig.class);

        binder.bind(TrinoCatalogFactory.class).to(TrinoMeteorCatalogFactory.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTableOperationsProvider.class).to(MeteorIcebergTableOperationsProvider.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("meteor-catalog-client", ForMeteorCatalog.class);
    }

    @Singleton
    @Provides
    private MeteorCatalogClient getMeteorCatalogClient(MeteorCatalogConfig config, @ForMeteorCatalog HttpClient client)
    {
        return new MeteorCatalogClient(client, config.getCatalogUri());
    }
}
