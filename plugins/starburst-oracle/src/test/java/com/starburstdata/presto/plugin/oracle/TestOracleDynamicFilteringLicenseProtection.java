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

import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringLicenseProtectionTest;
import io.prestosql.testing.QueryRunner;

import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.connectionProperties;

public class TestOracleDynamicFilteringLicenseProtection
        extends AbstractDynamicFilteringLicenseProtectionTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(false)
                .withConnectorProperties(connectionProperties())
                .build();
    }
}