/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerQueryRunner.ALICE_USER;
import static com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerQueryRunner.BOB_USER;
import static com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerQueryRunner.CHARLIE_USER;
import static com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerQueryRunner.UNKNOWN_USER;
import static com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerQueryRunner.createSession;

public class TestSqlServerImpersonationWithAuthToLocal
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer());

        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withEnterpriseFeatures()
                .withSessionModifier(session -> createSession(ALICE_USER + "/admin@company.com"))
                .withImpersonation("auth-to-local.json")
                .build();
    }

    @Test
    public void testUserImpersonation()
    {
        assertQuery(
                createSession(ALICE_USER + "/admin@company.com"),
                "SELECT * FROM user_context",
                "SELECT 'alice_login', 'sa', 'alice_login', 'alice', 'alice'");
        assertQuery(
                createSession(BOB_USER + "/user@company.com"),
                "SELECT * FROM user_context",
                "SELECT 'bob_login', 'sa', 'bob_login', 'bob', 'bob'");
        assertQueryFails(
                createSession(CHARLIE_USER + "/hr@company.com"),
                "SELECT * FROM user_context",
                "line 1:15: Table 'sqlserver.dbo.user_context' does not exist");
        assertQueryFails(
                createSession(UNKNOWN_USER + "/marketing@company.com"),
                "SELECT * FROM user_context",
                ".*database principal .* does not exist.*");
        assertQueryFails(
                createSession(UNKNOWN_USER + "/marketing@other.com"),
                "SELECT * FROM user_context",
                "No auth-to-local rule was found for user \\[non_existing_user/marketing@other.com] and principal \\[non_existing_user/marketing@other.com]");
        assertQueryFails(
                createSession(BOB_USER),
                "SELECT * FROM user_context",
                "No auth-to-local rule was found for user \\[bob] and principal \\[bob]");
    }
}
