/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import io.prestosql.testing.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.ALICE_USER;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.BOB_USER;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.CHARLIE_USER;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.UNKNOWN_USER;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

@Test
public abstract class BaseOracleImpersonationWithAuthToLocal
        extends AbstractTestQueryFramework
{
    protected abstract String getProxyUser();

    @Test
    public void testImpersonation()
    {
        String proxyUser = getProxyUser().toUpperCase(ENGLISH);
        assertQueryFails(
                createSession(ALICE_USER),
                "SELECT * FROM user_context",
                "No auth-to-local rule was found for user \\[alice\\] and principal is missing");
        assertQuery(
                createSession(ALICE_USER + "/admin@company.com"),
                "SELECT * FROM user_context",
                format("SELECT 'ALICE', 'ALICE', 'ALICE', '%s'", proxyUser));
        assertQuery(
                createSession(BOB_USER + "/market@company.com"),
                "SELECT * FROM user_context",
                format("SELECT 'BOB', 'BOB', 'BOB', '%s'", proxyUser));
        assertQueryFails(
                createSession(CHARLIE_USER + "/hr@company.com"),
                "SELECT * FROM user_context",
                ".*Table 'oracle.presto_test_user.user_context' does not exist");
        assertQueryFails(
                createSession(UNKNOWN_USER + "/x@company.com"),
                "SELECT * FROM user_context",
                "ORA-01017: invalid username/password; logon denied\n");
        assertQueryFails(
                createSession(UNKNOWN_USER + "/x@other.com"),
                "SELECT * FROM user_context",
                "No auth-to-local rule was found for user \\[non_existing_user/x@other.com\\] and principal is missing");
    }
}