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

import com.starburstdata.trino.plugin.stargate.StargateQueryRunner;
import io.trino.testing.DistributedQueryRunner;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunner;
import static io.airlift.testing.Closeables.closeAllSuppress;

public final class GalaxyStargateQueryRunner
{
    private GalaxyStargateQueryRunner() {}

    private static void addStarburstEnterpriseToRemoteStarburstQueryRunner(
            DistributedQueryRunner queryRunner,
            Map<String, String> connectorProperties)
            throws Exception
    {
        try {
            queryRunner.installPlugin(new GalaxyStargatePlugin());
            queryRunner.createCatalog("stargate", "stargate", connectorProperties);
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    public static DistributedQueryRunner createRemoteStarburstQueryRunnerWithStarburstEnterprise(Map<String, String> connectorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createRemoteStarburstQueryRunner(Optional.empty());
        addStarburstEnterpriseToRemoteStarburstQueryRunner(queryRunner, connectorProperties);
        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        StargateQueryRunner.main(args);
    }
}
