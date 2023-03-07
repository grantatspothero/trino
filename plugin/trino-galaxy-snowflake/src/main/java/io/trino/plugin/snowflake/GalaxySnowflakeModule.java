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
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.snowflake.client.jdbc.SnowflakeDriver;
import com.starburstdata.trino.plugins.snowflake.SnowflakeConfig;
import com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcClientModule;
import com.starburstdata.trino.plugins.snowflake.jdbc.WarehouseAwareDriverConnectionFactory;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import io.trino.plugin.base.galaxy.RegionEnforcementConfig;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.CatalogHandle;

import javax.inject.Qualifier;
import javax.inject.Singleton;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Properties;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogId;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogName;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCrossRegionAllowed;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addRegionLocalIpAddresses;

public class GalaxySnowflakeModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new SnowflakeJdbcClientModule(false));

        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setBinding()
                .to(Key.get(ConnectionFactory.class, ForSnowflake.class))
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForSnowflake
    public static ConnectionFactory createConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            SnowflakeConfig snowflakeConfig,
            RegionEnforcementConfig regionEnforcementConfig)
    {
        Properties properties = SnowflakeJdbcClientModule.getConnectionProperties(snowflakeConfig);

        properties.setProperty("socketFactory", GalaxySqlSocketFactory.class.getName());
        addCatalogName(properties, catalogHandle.getCatalogName());
        addCatalogId(properties, catalogHandle.getVersion().toString());
        addCrossRegionAllowed(properties, regionEnforcementConfig.getAllowCrossRegionAccess());
        addRegionLocalIpAddresses(properties, regionEnforcementConfig.getAllowedIpAddresses());

        return new WarehouseAwareDriverConnectionFactory(
                new SnowflakeDriver(),
                config.getConnectionUrl(),
                properties,
                credentialProvider);
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Qualifier
    public @interface ForSnowflake {}
}
