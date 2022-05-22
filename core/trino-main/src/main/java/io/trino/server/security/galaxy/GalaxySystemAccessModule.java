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
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.connector.CatalogManagerConfig;
import io.trino.connector.CatalogManagerConfig.CatalogMangerKind;
import io.trino.security.AccessControlManager;
import io.trino.security.DefaultSystemAccessControlName;
import io.trino.server.galaxy.GalaxyAuthorizationClientModule;
import io.trino.spi.security.SystemAccessControlFactory;

import javax.inject.Inject;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public class GalaxySystemAccessModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        CatalogManagerConfig catalogManagerConfig = buildConfigObject(CatalogManagerConfig.class);
        if (catalogManagerConfig.getCatalogMangerKind() == CatalogMangerKind.METADATA_ONLY) {
            newOptionalBinder(binder, Key.get(String.class, DefaultSystemAccessControlName.class)).setBinding().toInstance(GalaxyMetadataSystemAccessFactory.NAME);
            GalaxyAuthorizationClientModule.bindHttpClient(binder);

            MetadataAccessControllerSupplier controllerSupplier = new MetadataAccessControllerSupplier();
            binder.bind(SystemAccessControlFactory.class).annotatedWith(ForGalaxySystemAccessControl.class).to(GalaxyMetadataSystemAccessFactory.class);
            binder.bind(MetadataAccessControllerSupplier.class).toInstance(controllerSupplier);
        }
        else {
            binder.bind(SystemAccessControlFactory.class).annotatedWith(ForGalaxySystemAccessControl.class).to(GalaxyTrinoSystemAccessFactory.class);
        }
        binder.bind(LazyRegistration.class).in(Scopes.SINGLETON);
    }

    private static class LazyRegistration
    {
        @Inject
        public LazyRegistration(AccessControlManager accessControlManager, @ForGalaxySystemAccessControl SystemAccessControlFactory factory)
        {
            requireNonNull(accessControlManager, "accessControlManager is null").addSystemAccessControlFactory(factory);
        }
    }
}
