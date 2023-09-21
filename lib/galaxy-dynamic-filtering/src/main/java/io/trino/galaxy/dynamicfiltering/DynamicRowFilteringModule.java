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
package io.trino.galaxy.dynamicfiltering;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.starburstdata.trino.plugins.dynamicfiltering.DynamicPageFilterCache;
import com.starburstdata.trino.plugins.dynamicfiltering.DynamicRowFilteringConfig;
import com.starburstdata.trino.plugins.dynamicfiltering.DynamicRowFilteringSessionProperties;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/*
 * Copy of DynamicRowFilteringModule from Starburst Trino plugins repo because adjustments
 * to {@link ConnectorPageSourceProvider} are required for Galaxy SPI.
 */
public class DynamicRowFilteringModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        Provider<CatalogName> catalogName = binder.getProvider(CatalogName.class);
        configBinder(binder).bindConfig(DynamicRowFilteringConfig.class);
        binder.bind(DynamicPageFilterCache.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DynamicPageFilterCache.class)
                .as(generator -> generator.generatedNameOf(DynamicPageFilterCache.class, catalogName.get().toString()));
        Multibinder<SessionPropertiesProvider> sessionProperties = newSetBinder(binder, SessionPropertiesProvider.class);
        sessionProperties.addBinding().to(DynamicRowFilteringSessionProperties.class).in(SINGLETON);

        newOptionalBinder(binder, ConnectorPageSourceProvider.class)
                .setBinding().to(DynamicRowFilteringPageSourceProvider.class).in(SINGLETON);
    }
}
