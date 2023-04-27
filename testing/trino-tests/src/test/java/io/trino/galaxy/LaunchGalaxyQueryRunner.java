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
package io.trino.galaxy;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.id.RoleId;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.security.galaxy.IdeTestingAccountFactory;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.tpch.TpchTable;

import static io.trino.testing.QueryAssertions.copyTpchTables;
import static java.lang.String.format;

/**
 * You can debug GalaxyQueryRunner queries in both Trino and Stargate portal-server!
 * <p>
 * To run the Trino CLI, allowing breakpoints in both Trino and portal-server:
 * <ul>
 *     <li>Debug main class LocalTestingTrinoPortalServer in the stargate project.  No args are required.</li>
 *     <li>Debug GalaxyQueryRunner with VM argument -DdebugPortal=true</li>
 * </ul>
 * <p>
 * To run Trino test TestGalaxyQueries, allowing breakpoints in both Trino and portal-server:
 * <ul>
 *     <li>Debug main class LocalTestingTrinoPortalServer in the stargate project.  No args are required.</li>
 *     <li>Debug TestGalaxyQueries with VM argument -DdebugPortal=true</li>
 * </ul>
 */
public final class LaunchGalaxyQueryRunner
{
    private static final Logger log = Logger.get(LaunchGalaxyQueryRunner.class);

    private LaunchGalaxyQueryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        long start = System.nanoTime();
        Logging.initialize();

        // There is a docker version available also
        TestingAccountFactory testingAccountFactory = new IdeTestingAccountFactory();
        TestingAccountClient account = testingAccountFactory.createAccountClient();
        DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder("memory", "tiny")
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .addPlugin(new TpchPlugin())
                .addCatalog("tpch", "tpch", ImmutableMap.of())
                .addPlugin(new MemoryPlugin())
                .addCatalog("memory", "memory", ImmutableMap.of())
                .setAccountClient(account)
                .setNodeCount(1)
                .build();
        queryRunner.execute(format("CREATE SCHEMA %s.%s", "memory", "tiny"));
        copyTpchTables(queryRunner, "tpch", "tiny", queryRunner.getDefaultSession(), TpchTable.getTables());

        Thread.sleep(10);
        log.info(
                "\n\n======== SERVER STARTED in %s ========\n" +
                        "Admin:\n" +
                        "  %s\n" +
                        "Public:\n" +
                        "  %s\n" +
                        "\n" +
                        "NOTE: the UI does not work due to authentication\n" +
                        "====",
                Duration.nanosSince(start),
                createCliCommand(queryRunner, account, account.getAdminRoleId()),
                createCliCommand(queryRunner, account, account.getPublicRoleId()));
    }

    private static String createCliCommand(DistributedQueryRunner queryRunner, TestingAccountClient account, RoleId roleId)
    {
        return String.format(
                "client/trino-cli/target/trino-cli-*-executable.jar" +
                        " --server %s" +
                        " --user=%s" +
                        " --extra-credential=accountId=%s" +
                        " --extra-credential=userId=%s" +
                        " --extra-credential=GalaxyTokenCredential=%s" +
                        " --extra-credential=roleId=%s",
                queryRunner.getCoordinator().getBaseUrl(),
                queryRunner.getDefaultSession().getUser(),
                account.getAccountId(),
                account.getAdminUserId(),
                account.getAdminTrinoAccessToken(),
                roleId);
    }
}
