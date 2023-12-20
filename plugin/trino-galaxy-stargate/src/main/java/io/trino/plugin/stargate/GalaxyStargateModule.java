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
package io.trino.plugin.stargate;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.trino.plugin.stargate.EnableWrites;
import com.starburstdata.trino.plugin.stargate.StargateModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ConfiguringConnectionFactory;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataConfig;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcMetadataConfig.ListColumnsMode.DMA;
import static java.util.Objects.requireNonNull;

public class GalaxyStargateModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        install(new StargateModule());

        newOptionalBinder(binder, Key.get(JdbcClient.class, ForBaseJdbc.class)).setBinding().to(GalaxyStargateClient.class).in(Scopes.SINGLETON);

        install(new GalaxyStargateAuthenticationModule());
        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setBinding()
                .to(Key.get(ConnectionFactory.class, ForStargate.class))
                .in(Scopes.SINGLETON);

        // DMA is a safe bet for Stargate connector, since we know exactly how the other system will behave.
        // TODO DMA_P may actually perform better, especially if the remote is a data lake connector, which will iterate over schemas anyway.
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setListColumnsMode(DMA));
    }

    @Provides
    @Singleton
    @ForStargate
    public ConnectionFactory getConnectionFactory(@GalaxyTransportConnectionFactory ConnectionFactory delegate, @EnableWrites boolean enableWrites)
    {
        requireNonNull(delegate, "delegate is null");
        if (enableWrites) {
            return delegate;
        }
        return new ConfiguringConnectionFactory(delegate, connection -> connection.setReadOnly(true));
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @BindingAnnotation
    public @interface ForStargate {}
}
