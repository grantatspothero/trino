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
package io.trino.tests.exchange.buffer;

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangePlugin;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.Closeables.closeAllSuppress;

public abstract class AbstractBufferExchangeDistributedEngineOnlyQueries
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingBufferServiceCluster bufferServiceCluster = TestingBufferServiceCluster.builder()
                .withImages("179619298502.dkr.ecr.us-east-1.amazonaws.com/galaxy/trino-buffer-service", getBufferServiceVersion())
                .build();
        closeAfterClass(bufferServiceCluster);

        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.buffer-discovery.uri", bufferServiceCluster.getDiscoveryUri())
                .put("exchange.sink-target-written-pages-count", "3") // small requests for better test coverage
                .put("exchange.source-handle-target-chunks-count", "4") // smaller handles make more sense for test env when we do not have too much data
                .put("exchange.min-buffer-nodes-per-partition", "2")
                .put("exchange.max-buffer-nodes-per-partition", "2")
                .buildOrThrow();

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new BufferExchangePlugin());
                    runner.loadExchangeManager("buffer", exchangeManagerProperties);
                })
                .setInitialTables(TpchTable.getTables())
                .build();

        queryRunner.getCoordinator().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        try {
            queryRunner.installPlugin(new MockConnectorPlugin(
                    MockConnectorFactory.builder()
                            .withSessionProperties(TEST_CATALOG_PROPERTIES)
                            .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock");
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    protected abstract String getBufferServiceVersion();

    @Override
    @Test
    public void testExplainAnalyzeVerbose()
    {
        // Spooling exchange does not provide output buffer utilization histogram
        Assertions.assertThatThrownBy(super::testExplainAnalyzeVerbose)
                .isInstanceOf(AssertionError.class)
                .hasMessageMatching("(?s).*Expecting actual:.*to contain pattern:.*");
    }

    @Override
    @Test
    @Disabled
    public void testSelectiveLimit()
    {
        // FTE mode does not terminate query when limit is reached
    }
}
