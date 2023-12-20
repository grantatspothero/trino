/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
