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

package io.trino.plugin.snowflake;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.snowflake.client.jdbc.SnowflakeDriver;
import com.starburstdata.trino.plugin.snowflake.SnowflakeConfig;
import com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeJdbcClientModule;
import com.starburstdata.trino.plugin.snowflake.parallel.ParallelWarehouseAwareDriverConnectionFactory;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.starburst.stargate.accesscontrol.client.HttpTrinoLocationClient;
import io.starburst.stargate.accesscontrol.client.HttpTrinoSecurityClient;
import io.starburst.stargate.accesscontrol.client.TrinoLocationApi;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.objectstore.GalaxyLocationAccessControl;
import io.trino.plugin.objectstore.GalaxyTrinoSecurityControl;
import io.trino.plugin.objectstore.TrinoSecurityControl;
import io.trino.plugin.snowflake.procedure.RefreshTableProcedure;
import io.trino.plugin.snowflake.procedure.RegisterTableProcedure;
import io.trino.plugin.snowflake.procedure.UnregisterTableProcedure;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.procedure.Procedure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GalaxySnowflakeParallelModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setBinding()
                .to(Key.get(ConnectionFactory.class, ForSnowflake.class))
                .in(Scopes.SINGLETON);

        // TODO: move TrinoSecurityControl out of objectstore module
        newOptionalBinder(binder, LocationAccessControl.class)
                .setBinding().to(GalaxyLocationAccessControl.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, TrinoSecurityControl.class)
                .setBinding().to(GalaxyTrinoSecurityControl.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("galaxy-security", ForGalaxySecurity.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(RegisterTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(RefreshTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(UnregisterTableProcedure.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static TrinoLocationApi createTrinoLocationApi(@ForGalaxySecurity HttpClient httpClient, GalaxySnowflakeConfig config)
    {
        return new HttpTrinoLocationClient(config.getAccountUri(), httpClient);
    }

    @Provides
    @Singleton
    public static TrinoSecurityApi createTrinoSecurityApi(@ForGalaxySecurity HttpClient httpClient, GalaxySnowflakeConfig config)
    {
        return new HttpTrinoSecurityClient(config.getAccessControlUri().orElse(config.getAccountUri()), httpClient);
    }

    @Provides
    @Singleton
    @ForSnowflake
    public static ConnectionFactory createConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            SnowflakeConfig snowflakeConfig,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig)
    {
        Properties properties = new SnowflakeJdbcClientModule.SnowflakeDefaultConnectionPropertiesProvider(snowflakeConfig).get();

        properties.setProperty("socketFactory", GalaxySqlSocketFactory.class.getName());
        RegionVerifierProperties.addRegionVerifierProperties(properties::setProperty, RegionVerifierProperties.generateFrom(localRegionConfig, crossRegionConfig));
        CatalogNetworkMonitorProperties.addCatalogNetworkMonitorProperties(properties::setProperty, CatalogNetworkMonitorProperties.generateFrom(crossRegionConfig, catalogHandle));

        return new ParallelWarehouseAwareDriverConnectionFactory(
                new SnowflakeDriver(),
                config.getConnectionUrl(),
                properties,
                credentialProvider);
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @BindingAnnotation
    public @interface ForSnowflake {}

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForGalaxySecurity {}
}
