/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.starburstdata.presto.plugin.postgresql.StarburstPostgreSqlPlugin;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.hive.HiveHadoop2Plugin;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.SystemAccessControl;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public final class StarburstRemoteQueryRunner
{
    private StarburstRemoteQueryRunner() {}

    private static DistributedQueryRunner createStarburstRemoteQueryRunner(Map<String, String> extraProperties, Optional<SystemAccessControl> systemAccessControl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            Session session = testSessionBuilder()
                    // Require explicit table qualification or custom session.
                    .setCatalog("unspecified_catalog")
                    .setSchema("unspecified_schema")
                    .build();
            DistributedQueryRunner.Builder queryRunnerBuilder = DistributedQueryRunner.builder(session)
                    .setNodeCount(1) // 1 is perfectly enough until we do parallel Starburst Remote connector
                    .setExtraProperties(extraProperties);

            systemAccessControl.ifPresent(queryRunnerBuilder::setSystemAccessControl);

            queryRunner = queryRunnerBuilder.build();
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            return queryRunner;
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static void addMemoryToStarburstRemoteQueryRunner(
            DistributedQueryRunner queryRunner,
            Iterable<TpchTable<?>> requiredTablesInMemoryConnector)
            throws Exception
    {
        try {
            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog("memory", "memory");

            queryRunner.execute("CREATE SCHEMA memory.tiny");
            Session tpchSetupSession = testSessionBuilder()
                    .setCatalog("memory")
                    .setSchema("tiny")
                    .build();
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, tpchSetupSession, requiredTablesInMemoryConnector);
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static void addHiveToStarburstRemoteQueryRunner(
            DistributedQueryRunner queryRunner,
            File tmpDir,
            Iterable<TpchTable<?>> requiredTablesInHiveConnector)
            throws Exception
    {
        try {
            queryRunner.installPlugin(new HiveHadoop2Plugin());
            queryRunner.createCatalog("hive", "hive-hadoop2", ImmutableMap.of(
                    "hive.metastore", "file",
                    "hive.metastore.catalog.dir", tmpDir.toURI().toString(),
                    "hive.security", "allow-all"));

            queryRunner.execute("CREATE SCHEMA hive.tiny");
            Session tpchSetupSession = testSessionBuilder()
                    .setCatalog("hive")
                    .setSchema("tiny")
                    .build();
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, tpchSetupSession, requiredTablesInHiveConnector);
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static void addPostgreSqlToStarburstRemoteQueryRunner(
            DistributedQueryRunner queryRunner,
            TestingPostgreSqlServer server,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> requiredTablesInPostgreSqlConnector)
            throws Exception
    {
        try {
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", server.getUser());
            connectorProperties.putIfAbsent("connection-password", server.getPassword());
            connectorProperties.putIfAbsent("allow-drop-table", "true");
            connectorProperties.putIfAbsent("postgresql.include-system-tables", "true");

            server.execute("CREATE SCHEMA tiny");

            queryRunner.installPlugin(new StarburstPostgreSqlPlugin());
            queryRunner.createCatalog("postgresql", "postgresql", connectorProperties);

            Session tpchSetupSession = testSessionBuilder()
                    .setCatalog("postgresql")
                    .setSchema("tiny")
                    .build();
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, tpchSetupSession, requiredTablesInPostgreSqlConnector);
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    public static DistributedQueryRunner createStarburstRemoteQueryRunnerWithMemory(
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> requiredTablesInMemoryConnector,
            Optional<SystemAccessControl> systemAccessControl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createStarburstRemoteQueryRunner(extraProperties, systemAccessControl);
        addMemoryToStarburstRemoteQueryRunner(queryRunner, requiredTablesInMemoryConnector);
        return queryRunner;
    }

    public static DistributedQueryRunner createStarburstRemoteQueryRunnerWithHive(
            File tmpDir,
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> requiredTablesInHiveConnector,
            Optional<SystemAccessControl> systemAccessControl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createStarburstRemoteQueryRunner(extraProperties, systemAccessControl);
        addHiveToStarburstRemoteQueryRunner(queryRunner, tmpDir, requiredTablesInHiveConnector);
        return queryRunner;
    }

    public static DistributedQueryRunner createStarburstRemoteQueryRunnerWithPostgreSql(
            TestingPostgreSqlServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> requiredTablesInPostgreSqlConnector,
            Optional<SystemAccessControl> systemAccessControl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createStarburstRemoteQueryRunner(extraProperties, systemAccessControl);
        addPostgreSqlToStarburstRemoteQueryRunner(queryRunner, server, connectorProperties, requiredTablesInPostgreSqlConnector);
        return queryRunner;
    }

    public static DistributedQueryRunner createStarburstRemoteQueryRunner(
            boolean enableWrites,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("p2p_remote")
                .setSchema("tiny")
                .build();

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(session)
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-user", "p2p");

            queryRunner.installPlugin(new TestingStarburstRemotePlugin(enableWrites));
            queryRunner.createCatalog("p2p_remote", "starburst-remote", connectorProperties);

            return queryRunner;
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    public static String starburstRemoteConnectionUrl(DistributedQueryRunner remoteStarburst, String catalog)
    {
        return connectionUrl(remoteStarburst.getCoordinator().getBaseUrl(), catalog);
    }

    private static String connectionUrl(URI trinoUri, String catalog)
    {
        verify(Objects.equals(trinoUri.getScheme(), "http"), "Unsupported scheme: %s", trinoUri.getScheme());
        verify(trinoUri.getUserInfo() == null, "Unsupported user info: %s", trinoUri.getUserInfo());
        verify(Objects.equals(trinoUri.getPath(), ""), "Unsupported path: %s", trinoUri.getPath());
        verify(trinoUri.getQuery() == null, "Unsupported query: %s", trinoUri.getQuery());
        verify(trinoUri.getFragment() == null, "Unsupported fragment: %s", trinoUri.getFragment());

        return format("jdbc:trino://%s/%s", trinoUri.getAuthority(), catalog);
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner remoteStarburst = createStarburstRemoteQueryRunner(Map.of(), Optional.empty());

        addMemoryToStarburstRemoteQueryRunner(
                remoteStarburst,
                TpchTable.getTables());

        TestingPostgreSqlServer postgreSqlServer = new TestingPostgreSqlServer();
        addPostgreSqlToStarburstRemoteQueryRunner(
                remoteStarburst,
                postgreSqlServer,
                Map.of("connection-url", postgreSqlServer.getJdbcUrl()),
                TpchTable.getTables());

        File tempDir = Files.createTempDir();
        addHiveToStarburstRemoteQueryRunner(
                remoteStarburst,
                tempDir,
                TpchTable.getTables());

        DistributedQueryRunner queryRunner = createStarburstRemoteQueryRunner(
                true,
                Map.of("http-server.http.port", "8080"),
                Map.of(
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, "memory"),
                        "allow-drop-table", "true"));
        queryRunner.createCatalog(
                "p2p_remote_postgresql",
                "starburst-remote",
                Map.of(
                        "connection-user", "p2p",
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, "postgresql"),
                        "allow-drop-table", "true"));
        queryRunner.createCatalog(
                "p2p_remote_hive",
                "starburst-remote",
                Map.of(
                        "connection-user", "p2p",
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, "hive"),
                        "allow-drop-table", "true"));

        Logger log = Logger.get(StarburstRemoteQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\nRemote Starburst: %s\n====", remoteStarburst.getCoordinator().getBaseUrl());
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}