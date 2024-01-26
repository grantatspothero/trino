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

import com.google.common.collect.ImmutableMap;

public class TestOracleImpersonationWithAuthToLocalWithoutPooling
        extends BaseOracleImpersonationWithAuthToLocalTest
{
    public TestOracleImpersonationWithAuthToLocalWithoutPooling()
    {
        super(ImmutableMap.<String, String>builder()
                .put("oracle.connection-pool.enabled", "false")
                .buildOrThrow());
    }
}
