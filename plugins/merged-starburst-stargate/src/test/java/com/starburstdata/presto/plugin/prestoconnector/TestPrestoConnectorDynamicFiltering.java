/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import org.testng.SkipException;

import java.util.List;
import java.util.Map;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunnerWithMemory;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrestoConnectorDynamicFiltering
        extends AbstractDynamicFilteringTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remotePresto = closeAfterClass(createRemotePrestoQueryRunnerWithMemory(
                Map.of(),
                List.of(ORDERS)));
        return createPrestoConnectorQueryRunner(
                false,
                Map.of(),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "memory"),
                        "allow-drop-table", "true"));
    }

    @Override
    protected boolean supportsSplitDynamicFiltering()
    {
        // JDBC connectors always generate single split
        // TODO https://starburstdata.atlassian.net/browse/PRESTO-4769 revisit in parallel Presto Connector
        return false;
    }

    @Override
    public void testDynamicFilteringDomainCompactionThreshold()
    {
        assertThatThrownBy(super::testDynamicFilteringDomainCompactionThreshold)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("not supported");
    }
}