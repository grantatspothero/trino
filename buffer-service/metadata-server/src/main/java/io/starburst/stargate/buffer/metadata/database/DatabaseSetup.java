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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import org.flywaydb.core.Flyway;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.configuration.ConfigurationLoader.loadProperties;

public class DatabaseSetup
{
    private static final Logger log = Logger.get(DatabaseSetup.class);

    private final Flyway flyway;

    @Inject
    public DatabaseSetup(DatabaseConfig config)
    {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl(config.getUrl());
        dataSource.setUser(config.getUser());
        dataSource.setPassword(config.getPassword());

        flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("/db")
                .ignoreFutureMigrations(true)
                .failOnMissingLocations(true)
                .cleanDisabled(true)
                .createSchemas(false)
                .group(false)
                .load();
    }

    public void migrate()
    {
        flyway.migrate();
    }

    public static void setupDatabase()
            throws IOException
    {
        setupDatabase(ImmutableMap.of(), true);
    }

    public static void setupDatabase(Map<String, String> properties)
            throws IOException
    {
        setupDatabase(properties, false);
    }

    private static void setupDatabase(Map<String, String> properties, boolean loadProperties)
            throws IOException
    {
        Bootstrap app = new Bootstrap(binder -> {
            configBinder(binder).bindConfig(DatabaseConfig.class);
            binder.bind(DatabaseSetup.class);
        });

        app = app.setRequiredConfigurationProperties(properties);
        if (loadProperties) {
            app = app.setOptionalConfigurationProperties(loadProperties());
        }
        Injector injector = app.initialize();

        log.info("Starting DB migrations...");
        injector.getInstance(DatabaseSetup.class).migrate();
        log.info("Finished DB migrations");
    }
}
