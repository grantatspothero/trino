/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.testing.TestingBufferService;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTestingBufferDistributedFaultTolerantEngineOnlyQueries
        extends AbstractDistributedEngineOnlyQueries
{
    private static final Logger log = Logger.get(TestTestingBufferDistributedFaultTolerantEngineOnlyQueries.class);

    private TestingBufferService bufferService;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long memoryHeadroom = (long) (0.7 * maxMemory);
        int dataServersCount = 3;
        log.info("Starting testing buffer service with %d data nodes; each with %s memory", dataServersCount, DataSize.succinctBytes(maxMemory - memoryHeadroom));

        bufferService = TestingBufferService
                .builder()
                .withDiscoveryServerBuilder(
                        builder -> builder.setConfigProperty("buffer.discovery.start-grace-period", "3s"))
                .withDataServerBuilder(
                        builder -> builder
                                .setConfigProperty("memory.heap-headroom", DataSize.succinctBytes(memoryHeadroom).toString())
                                .setConfigProperty("exchange.staleness-threshold", "2h")) //tmp
                .setDataServersCount(dataServersCount)
                .build();
        URI discoveryServerUri = bufferService.getDiscoveryServer().getBaseUri();

        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.buffer-discovery.uri", discoveryServerUri.toString())
                .put("exchange.sink-target-written-pages-count", "3") // small requests for better test coverage
                .put("exchange.source-handle-target-chunks-count", "4") // smaller handles make more sense for test env when we do not have too much data
                .put("exchange.min-buffer-nodes-per-partition", "2")
                .put("exchange.max-buffer-nodes-per-partition", "2")
                .buildOrThrow();

        Map<String, String> extraProperties = new HashMap<>(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        // By default, FaultTolerantExecutionConnectorTestHelper.getExtraProperties sets
        // executor-pool-size to 10. Such small value may cause queries to fail if tests are run in parallel.
        // The reason for that is currently same thread pool is used for long running jobs driving EventDrivenFaultTolerantQueryScheduler
        // as well as for future callback used during query processing. If all threads are used by EventDrivenFaultTolerantQueryScheduler jobs
        // then callbacks would not be executed and queries may get blocked.
        // TODO: update code in Trino so it is not needed
        extraProperties.put("query.executor-pool-size", "100");
        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(extraProperties)
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

    @Test
    @Timeout(60)
    public void stressUnionWithLimit()
    {
        // regression test for query stuck because future returned by BufferExchangeSource.isBlocked() was not completed
        // in BufferExchangeSource.close() which was called before all data was produced by source
        for (int i = 0; i < 10; i++) {
            assertThat(query("SELECT DISTINCT x FROM (SELECT custkey x FROM customer) UNION  (SELECT nationkey x FROM nation) LIMIT 1"))
                    .result().rowCount().isEqualTo(1);
        }
    }

    @Override
    @Test
    @Disabled
    public void testExplainAnalyzeVerbose()
    {
        // Spooling exchange does not prove output buffer utilization histogram
    }

    @Override
    @Test
    @Disabled
    public void testSelectiveLimit()
    {
        // FTE mode does not terminate query when limit is reached
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        if (bufferService != null) {
            bufferService.close();
        }
    }
}
