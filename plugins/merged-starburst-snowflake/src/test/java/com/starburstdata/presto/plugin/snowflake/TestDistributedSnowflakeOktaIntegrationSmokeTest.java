/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.oktaImpersonationEnabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_PASSWORD;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_USER;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestDistributedSnowflakeOktaIntegrationSmokeTest
        extends BaseSnowflakeIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
                .withServer(server)
                .withAdditionalProperties(oktaImpersonationEnabled(false))
                .withConnectionPooling()
                .build();
    }

    @Test
    public void testOktaWithoutConnectionPooling()
            throws Exception
    {
        try (QueryRunner queryRunner = distributedBuilder()
                .withAdditionalProperties(oktaImpersonationEnabled(false))
                .build()) {
            Session session = getSession();
            String tableName = "test_insert_" + randomTableSuffix();
            queryRunner.execute(session, format("CREATE TABLE test_schema.%s (x decimal(19, 0), y varchar(100))", tableName));
            queryRunner.execute(session, format("INSERT INTO %s VALUES (123, 'test')", tableName));
            queryRunner.execute(session, format("SELECT * FROM %s", tableName));
            queryRunner.execute(session, format("DROP TABLE test_schema.%s", tableName));
        }
    }

    @Override
    protected Session getSession()
    {
        return testSessionBuilder()
                .setCatalog("snowflake")
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.forUser(OKTA_USER)
                        .withExtraCredentials(ImmutableMap.of(
                                "okta.user", OKTA_USER,
                                "okta.password", OKTA_PASSWORD))
                        .build())
                .build();
    }
}