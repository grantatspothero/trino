/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Runnables;
import com.starburstdata.presto.license.LicenseManager;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.trino.plugins.oracle.OracleTestUsers.ALICE_USER;
import static com.starburstdata.trino.plugins.oracle.OracleTestUsers.USER;
import static com.starburstdata.trino.plugins.oracle.TestingStarburstOracleServer.connectionProperties;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTable;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class OracleQueryRunner
{
    public static final LicenseManager NOOP_LICENSE_MANAGER = () -> true;

    private static final Logger LOG = Logger.get(OracleQueryRunner.class);

    private static final String ORACLE_CATALOG = "oracle";

    private OracleQueryRunner() {}

    private static QueryRunner createOracleQueryRunner(
            boolean unlockEnterpriseFeatures,
            Plugin licensedPlugin,
            Map<String, String> connectorProperties,
            Function<Session, Session> sessionModifier,
            Iterable<TpchTable<?>> tables,
            int nodesCount,
            Map<String, String> coordinatorProperties,
            Runnable createUsers,
            Runnable provisionTables)
            throws Exception
    {
        Session session = sessionModifier.apply(createSession(ALICE_USER));
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(nodesCount)
                .setCoordinatorProperties(coordinatorProperties)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

            createUsers.run();

            if (unlockEnterpriseFeatures) {
                queryRunner.installPlugin(new TestingStarburstOraclePlugin(NOOP_LICENSE_MANAGER));
            }
            else {
                queryRunner.installPlugin(licensedPlugin);
            }

            queryRunner.createCatalog(ORACLE_CATALOG, ORACLE_CATALOG, connectorProperties);

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx", ImmutableMap.of());

            provisionTables(session, queryRunner, tables);

            provisionTables.run();
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    private static synchronized void provisionTables(Session session, QueryRunner queryRunner, Iterable<TpchTable<?>> tables)
    {
        Set<String> existingTables = queryRunner.listTables(session, ORACLE_CATALOG, session.getSchema().orElse(USER)).stream()
                .map(QualifiedObjectName::getObjectName)
                .collect(toImmutableSet());

        Streams.stream(tables)
                .filter(table -> !existingTables.contains(table.getTableName().toLowerCase(ENGLISH)))
                .forEach(table -> copyTable(queryRunner, "tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH), session));
    }

    public static Session createSession(String user)
    {
        return createSession(user, USER);
    }

    public static Session createSession(String user, String schema)
    {
        return testSessionBuilder()
                .setCatalog(ORACLE_CATALOG)
                .setSchema(schema)
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private boolean unlockEnterpriseFeatures;
        private Plugin licensedPlugin = new StarburstOraclePlugin(NOOP_LICENSE_MANAGER);
        private Map<String, String> connectorProperties = emptyMap();
        private Function<Session, Session> sessionModifier = Function.identity();
        private Iterable<TpchTable<?>> tables = ImmutableList.of();
        private int nodesCount = 3;
        private Map<String, String> coordinatorProperties = emptyMap();
        private Runnable createUsers = OracleTestUsers::createStandardUsers;
        private Runnable provisionTables = Runnables.doNothing();

        private Builder() {}

        public Builder withUnlockEnterpriseFeatures(boolean unlockEnterpriseFeatures)
        {
            this.unlockEnterpriseFeatures = unlockEnterpriseFeatures;
            return this;
        }

        public Builder withLicensedPlugin(Plugin plugin)
        {
            this.licensedPlugin = requireNonNull(plugin, "licensedPlugin is null");
            return this;
        }

        public Builder withConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties = updateProperties(this.connectorProperties, connectorProperties);
            return this;
        }

        public Builder withSessionModifier(Function<Session, Session> sessionModifier)
        {
            this.sessionModifier = requireNonNull(sessionModifier, "sessionModifier is null");
            return this;
        }

        public Builder withTables(Iterable<TpchTable<?>> tables)
        {
            this.tables = requireNonNull(tables, "tables is null");
            return this;
        }

        public Builder withNodesCount(int nodesCount)
        {
            verify(nodesCount > 0, "nodesCount should be greater than 0");
            this.nodesCount = nodesCount;
            return this;
        }

        public Builder withCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = updateProperties(this.coordinatorProperties, coordinatorProperties);
            return this;
        }

        public Builder withCreateUsers(Runnable runnable)
        {
            this.createUsers = requireNonNull(runnable, "createUsers is null");
            return this;
        }

        public Builder withProvisionTables(Runnable runnable)
        {
            this.provisionTables = requireNonNull(runnable, "provisionTables is null");
            return this;
        }

        public QueryRunner build()
                throws Exception
        {
            return createOracleQueryRunner(unlockEnterpriseFeatures, licensedPlugin, connectorProperties, sessionModifier, tables, nodesCount, coordinatorProperties, createUsers, provisionTables);
        }
    }

    private static Map<String, String> updateProperties(Map<String, String> properties, Map<String, String> update)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(requireNonNull(properties, "properties is null"))
                .putAll(requireNonNull(update, "update is null"))
                .buildOrThrow();
    }

    public static void main(String[] args)
            throws Exception
    {
        // using single node so JMX stats can be queried
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) OracleQueryRunner.builder()
                .withConnectorProperties(connectionProperties())
                .withNodesCount(1)
                .withCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .withTables(TpchTable.getTables())
                .build();

        LOG.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}