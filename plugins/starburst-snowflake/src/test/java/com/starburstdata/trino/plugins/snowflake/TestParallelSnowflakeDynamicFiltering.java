/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.parallelBuilder;

public class TestParallelSnowflakeDynamicFiltering
        extends TestSnowflakeDynamicFiltering
{
    @Override
    protected SnowflakeQueryRunner.Builder createBuilder()
    {
        return parallelBuilder();
    }
}