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

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.stargate.StargateQueryRunner;
import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.stargate.GalaxyStargateQueryRunner.createRemoteStarburstQueryRunnerWithStarburstEnterprise;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseStargateWithStarburstEnterpriseConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    private final String version;

    public BaseStargateWithStarburstEnterpriseConnectorSmokeTest(String version)
    {
        this.version = requireNonNull(version, "version is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingStarburstEnterpriseServer server = closeAfterClass(new TestingStarburstEnterpriseServer(version));

        DistributedQueryRunner remoteStarburst = closeAfterClass(createRemoteStarburstQueryRunnerWithStarburstEnterprise(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", server.withDatabaseName("tpch").getJdbcUrl())
                        .put("connection-user", server.getUsername())
                        .buildOrThrow()));

        return StargateQueryRunner.builder(remoteStarburst, "stargate").build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_CREATE_TABLE,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_INSERT,
                    SUPPORTS_DELETE,
                    SUPPORTS_ARRAY -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
    @Test
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessageContaining("This connector does not support creating schemas");
    }

    @Override // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
    @Test
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("This connector does not support renaming schemas");
    }
}
