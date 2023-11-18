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
package io.trino.plugin.objectstore.catalog.galaxy;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.plugin.hive.metastore.CachingHiveMetastoreModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.galaxy.ForGalaxyMetastore;
import io.trino.plugin.hive.metastore.galaxy.GalaxyMetastoreSessionProperties;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.MetastoreValidator;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.galaxy.GalaxyMetastoreOperationsProvider;
import io.trino.plugin.iceberg.catalog.galaxy.IcebergGalaxyCatalogConfig;
import io.trino.plugin.iceberg.catalog.galaxy.TrinoGalaxyCatalogFactory;
import io.trino.plugin.iceberg.procedure.MigrateProcedure;
import io.trino.spi.procedure.Procedure;

import java.util.concurrent.TimeUnit;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;

public class TestingIcebergGalaxyMetastoreCatalogModule
        extends AbstractConfigurationAwareModule
{
    private final HiveMetastore metastore;

    public TestingIcebergGalaxyMetastoreCatalogModule(HiveMetastore metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(HiveMetastoreFactory.class).annotatedWith(RawHiveMetastoreFactory.class).toInstance(HiveMetastoreFactory.ofInstance(metastore));

        binder.bind(boolean.class).annotatedWith(AllowHiveTableRename.class).toInstance(true);
        httpClientBinder(binder).bindHttpClient("galaxy-metastore", ForGalaxyMetastore.class);

        configBinder(binder).bindConfig(IcebergGalaxyCatalogConfig.class);
        binder.bind(IcebergTableOperationsProvider.class).to(GalaxyMetastoreOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(TrinoCatalogFactory.class).to(TrinoGalaxyCatalogFactory.class).in(Scopes.SINGLETON);
        binder.bind(MetastoreValidator.class).asEagerSingleton();
        install(new CachingHiveMetastoreModule(false));

        configBinder(binder).bindConfigDefaults(CachingHiveMetastoreConfig.class, config -> {
            // ensure caching metastore wrapper isn't created, as it's not leveraged by Iceberg
            config.setStatsCacheTtl(new Duration(0, TimeUnit.SECONDS));
        });

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(MigrateProcedure.class).in(Scopes.SINGLETON);

        newSetBinder(binder, SessionPropertiesProvider.class)
                .addBinding().to(GalaxyMetastoreSessionProperties.class).in(Scopes.SINGLETON);
    }
}
