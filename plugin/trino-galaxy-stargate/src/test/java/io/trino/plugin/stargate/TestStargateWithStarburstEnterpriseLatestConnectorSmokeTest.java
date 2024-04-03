/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.trino.plugin.stargate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStargateWithStarburstEnterpriseLatestConnectorSmokeTest
        extends BaseStargateWithStarburstEnterpriseConnectorSmokeTest
{
    public TestStargateWithStarburstEnterpriseLatestConnectorSmokeTest()
    {
        super("latest");
    }

    @Override // Column definitions contain NOT NULL constraint in newer Starburst Enterprise
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("""
                        CREATE TABLE p2p_remote.tiny.region (
                           regionkey bigint NOT NULL,
                           name varchar(25) NOT NULL,
                           comment varchar(152) NOT NULL
                        )""");
    }
}
