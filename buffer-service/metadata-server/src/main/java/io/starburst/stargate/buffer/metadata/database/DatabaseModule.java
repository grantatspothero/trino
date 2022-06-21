/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.metadata.database;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class DatabaseModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(DatabaseConfig.class);
    }

    @Provides
    public Jdbi create(DatabaseConfig config)
    {
        return Jdbi.create(config.getUrl(), config.getUser(), config.getPassword())
                .installPlugin(new SqlObjectPlugin());
    }
}
