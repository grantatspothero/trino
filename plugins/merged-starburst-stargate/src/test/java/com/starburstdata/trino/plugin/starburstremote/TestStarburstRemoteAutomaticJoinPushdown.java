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

import com.google.common.base.Strings;
import com.starburstdata.presto.plugin.jdbc.joinpushdown.BaseAutomaticJoinPushdownTest;
import io.trino.Session;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithPostgreSql;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;
import static java.lang.String.format;

public class TestStarburstRemoteAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    private TestingPostgreSqlServer postgreSqlServer;
    private DistributedQueryRunner remoteStarburst;
    private Session remoteSession;

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());

        remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithPostgreSql(
                postgreSqlServer,
                Map.of(),
                Map.of(
                        "connection-url", postgreSqlServer.getJdbcUrl(),
                        "connection-user", postgreSqlServer.getUser(),
                        "connection-password", postgreSqlServer.getPassword()),
                List.of(),
                Optional.empty()));

        remoteSession = Session.builder(remoteStarburst.getDefaultSession())
                .setCatalog("postgresql")
                .setSchema("tiny")
                .build();

        return createStarburstRemoteQueryRunner(
                false,
                Map.of(),
                Map.of(
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, "postgresql"),
                        "allow-drop-table", "true"));
    }

    @Override
    protected void gatherStats(String tableName)
    {
        onRemoteDatabase(handle -> {
            handle.execute("ANALYZE " + tableName);
            for (int i = 0; i < 5; i++) {
                long actualCount = handle.createQuery("SELECT count(*) FROM " + tableName).mapTo(Long.class).one();

                long estimatedCount = handle.createQuery(format("SELECT reltuples FROM pg_class WHERE oid = '%s'::regclass::oid", tableName))
                        .mapTo(Long.class)
                        .one();
                if (actualCount == estimatedCount) {
                    return;
                }
                handle.execute("ANALYZE " + tableName);
            }

            throw new IllegalStateException("Stats not gathered"); // for small test tables reltuples should be exact
        });
    }

    private <E extends Exception> void onRemoteDatabase(HandleConsumer<E> callback)
            throws E
    {
        Properties properties = new Properties();
        properties.setProperty("currentSchema", "tiny");
        properties.setProperty("user", postgreSqlServer.getUser());
        properties.setProperty("password", postgreSqlServer.getPassword());
        Jdbi.create(postgreSqlServer.getJdbcUrl(), properties)
                .useHandle(callback);
    }

    @Override
    protected TestTable joinTestTable(String name, long rowsCount, int keyDistinctValues, int paddingSize)
    {
        String padding = Strings.repeat("x", paddingSize);

        return new TestTable(
                sql -> remoteStarburst.execute(remoteSession, sql),
                name,
                format("(key, padding) AS SELECT mod(orderkey, %s), '%s' FROM tpch.sf100.orders LIMIT %s", keyDistinctValues, padding, rowsCount));
    }
}