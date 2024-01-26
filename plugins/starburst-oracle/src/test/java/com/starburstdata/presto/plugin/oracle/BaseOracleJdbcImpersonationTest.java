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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.Objects.requireNonNull;

@Test
public abstract class BaseOracleJdbcImpersonationTest
        extends BaseOracleImpersonationTest
{
    private final Map<String, String> additionalProperties;

    protected BaseOracleJdbcImpersonationTest(Map<String, String> additionalProperties)
    {
        this.additionalProperties = ImmutableMap.copyOf(requireNonNull(additionalProperties, "additionalProperties is null"));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .putAll(TestingStarburstOracleServer.connectionProperties())
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.synonyms.enabled", "true")
                .putAll(additionalProperties)
                .build();

        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(properties)
                .withTables(ImmutableList.of())
                .build();
    }

    @Override
    protected String getProxyUser()
    {
        return "presto_test_user";
    }
}
