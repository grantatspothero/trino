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

import com.google.common.collect.ImmutableMap;

public class TestOracleKerberosImpersonationWithAuthToLocalPooled
        extends BaseOracleKerberosImpersonationWithAuthToLocalTest
{
    public TestOracleKerberosImpersonationWithAuthToLocalPooled()
    {
        super(ImmutableMap.<String, String>builder().build());
    }
}