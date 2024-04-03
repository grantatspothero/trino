/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.trino.plugin.stargate;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.trino.plugin.stargate.EnableWrites;
import com.starburstdata.trino.plugin.stargate.StargateClient;
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
import static java.util.Objects.requireNonNull;

public class GalaxyStargateModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        install(new StargateModule());

        newOptionalBinder(binder, Key.get(JdbcClient.class, ForBaseJdbc.class)).setBinding().to(StargateClient.class).in(Scopes.SINGLETON);

        install(new GalaxyStargateAuthenticationModule());
        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setBinding()
                .to(Key.get(ConnectionFactory.class, ForStargate.class))
                .in(Scopes.SINGLETON);

        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setBulkListColumns(true));
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
