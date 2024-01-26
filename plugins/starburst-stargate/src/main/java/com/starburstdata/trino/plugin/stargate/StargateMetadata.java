/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.starburstdata.presto.plugin.jdbc.StarburstJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;

public class StargateMetadata
        extends StarburstJdbcMetadata
{
    public StargateMetadata(JdbcClient jdbcClient, boolean allowDropTable)
    {
        super(jdbcClient, allowDropTable);
    }

    @Override
    protected boolean precalculateStatisticsForPushdown()
    {
        // Remote cluster is very good at estimates, should not be inferior to ours.
        return false;
    }
}
