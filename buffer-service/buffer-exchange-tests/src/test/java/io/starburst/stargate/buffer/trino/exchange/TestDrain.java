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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.server.testing.TestingDataServer;
import io.starburst.stargate.buffer.testing.TestingBufferService;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.spi.type.BigintType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

public class TestDrain
{
    private static final Logger log = Logger.get(TestDrain.class);

    private ListeningExecutorService executor;

    private HttpClient httpClient;

    @BeforeClass
    public void setup()
    {
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        httpClient = new JettyHttpClient(new HttpClientConfig());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        closeAll(httpClient);
        httpClient = null;
    }

    @Test
    public void testDrainQuick()
            throws Exception
    {
        testDrain(3);
    }

    @Test(enabled = false) // too slow for automation
    public void testDrainStress()
            throws Exception
    {
        testDrain(20);
    }

    private void testDrain(int drainIterations)
            throws Exception
    {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long memoryHeadroom = (long) (0.9 * maxMemory);

        TestingBufferService bufferService = TestingBufferService
                .builder()
                .withDiscoveryServerBuilder(
                        builder -> builder.setConfigProperty("buffer.discovery.start-grace-period", "3s"))
                .withDataServerBuilder(
                        builder -> builder
                                .setConfigProperty("testing.enable-stats-logging", "false")
                                .setConfigProperty("memory.heap-headroom", DataSize.succinctBytes(memoryHeadroom).toString())
                                .setConfigProperty("draining.min-duration", "10s")
                                .setConfigProperty("exchange.staleness-threshold", "2h")) //tmp
                .setDataServersCount(3)
                .build();

        URI discoveryServerUri = bufferService.getDiscoveryServer().getBaseUri();

        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.buffer-discovery.uri", discoveryServerUri.toString())
                .put("exchange.sink-target-written-pages-count", "3") // small requests for better test coverage
                .put("exchange.sink-blocked-memory-low", "32MB")
                .put("exchange.sink-blocked-memory-high", "64MB")
                .put("exchange.source-handle-target-chunks-count", "4") // smaller handles make more sense for test env when we do not have too much data
                .put("exchange.min-buffer-nodes-per-partition", "2")
                .put("exchange.max-buffer-nodes-per-partition", "2")
                .put("exchange.buffer-data.spooling-storage-type", "LOCAL")
                .buildOrThrow();

        Map<String, String> runnerExtraProperties = new HashMap<>();
        runnerExtraProperties.putAll(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
        runnerExtraProperties.put("fault-tolerant-execution-partition-count", "50"); // use more partition to be sure we get some on the drained node
        runnerExtraProperties.put("optimizer.join-reordering-strategy", "NONE");
        runnerExtraProperties.put("join-distribution-type", "PARTITIONED");
        runnerExtraProperties.put("enable-dynamic-filtering", "false");

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(runnerExtraProperties)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new BufferExchangePlugin());
                    runner.loadExchangeManager("buffer", exchangeManagerProperties);
                })
                .setInitialTables(ImmutableList.of())
                .build();

        AtomicBoolean testInProgress = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(2);

        ListenableFuture<?> drainingJob = executor.submit(() -> {
            int drainCount = drainIterations;
            try {
                while (drainCount > 0 && testInProgress.get()) {
                    drainCount--;
                    TestingDataServer testingDataServer = bufferService.getDataServers().get(ThreadLocalRandom.current().nextInt(bufferService.getDataServers().size()));
                    Thread.sleep(10000);
                    drainAndShutdownDataServer(testingDataServer);
                    TestingDataServer addedDataServer;
                    synchronized (TestDrain.this) {
                        bufferService.removeDataServer(testingDataServer);
                        Thread.sleep(10000);
                        addedDataServer = bufferService.addDataServer();
                    }
                    awaitActive(addedDataServer);
                }
                testInProgress.set(false);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            finally {
                testInProgress.set(false);
                latch.countDown();
            }
        });

        ListenableFuture<?> queryJob = executor.submit(() -> {
            try {
                while (testInProgress.get()) {
                    MaterializedResult result = queryRunner.execute("""
                            WITH big        AS (SELECT custkey k FROM tpch.sf50.orders),
                                 single_row AS (SELECT custkey AS k FROM tpch.tiny.customer WHERE acctbal = 2237.64)
                            SELECT count(*) FROM single_row,big WHERE single_row.k = big.k""");
                    MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                            .row(19L)
                            .build();
                    assertThat(result).isEqualTo(expected);
                }
            }
            finally {
                testInProgress.set(false);
                latch.countDown();
            }
        });

        latch.await();

        assertThat(drainingJob).as("draining job").succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(queryJob).as("query job").succeedsWithin(5, TimeUnit.SECONDS);
    }

    private void drainAndShutdownDataServer(TestingDataServer dataServer)
    {
        // Trigger draining
        log.info("Draining data server %s", dataServer.getNodeId());
        URI baseUri = dataServer.getBaseUri();
        Request drainRequest = Request.Builder.prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(baseUri)
                        .replacePath("/api/v1/buffer/data/drain")
                        .build())
                .build();
        StatusResponseHandler.StatusResponse drainResponse = httpClient.execute(drainRequest, createStatusResponseHandler());

        assertThat(drainResponse.getStatusCode()).as("status code for /drain on data server %s", dataServer.getNodeId()).isEqualTo(200);

        // Wait until drained
        Request stateRequest = Request.Builder.prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(baseUri)
                        .replacePath("/api/v1/buffer/data/state")
                        .build())
                .build();

        await()
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .ignoreNoExceptions()
                .until(() -> {
                    StringResponseHandler.StringResponse stateResponse = httpClient.execute(stateRequest, createStringResponseHandler());
                    assertThat(stateResponse.getStatusCode()).as("status code for /state on data server %s", dataServer.getNodeId()).isEqualTo(200);
                    String currentState = stateResponse.getBody().trim();
                    log.info("got current state %s for data server %s", currentState, dataServer.getNodeId());
                    return currentState.equals("DRAINED");
                });

        // trigger shutdown
        log.info("Shutting down data server %s", dataServer.getNodeId());
        Request preShutdownRequest = Request.Builder.prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(baseUri)
                        .replacePath("/api/v1/buffer/data/preShutdown")
                        .build())
                .build();
        StatusResponseHandler.StatusResponse preShutdownResponse = httpClient.execute(
                preShutdownRequest,
                createStatusResponseHandler());

        assertThat(preShutdownResponse.getStatusCode()).as("status code for /drain on data server %s", dataServer.getNodeId()).isEqualTo(200);

        // ensure that data server stopped responding
        log.info("Waiting till data server %s is gone", dataServer.getNodeId());
        assertEventually(
                succinctDuration(5, TimeUnit.SECONDS),
                () -> assertThatThrownBy(() -> httpClient.execute(stateRequest, createStringResponseHandler()))
                        .as("connectivity test for data server %s", dataServer.getNodeId())
                        .hasMessageContaining("Server refused connection"));
    }

    private void awaitActive(TestingDataServer dataServer)
    {
        log.info("Waiting till data server %s is ACTIVE", dataServer.getNodeId());
        URI baseUri = dataServer.getBaseUri();
        Request stateRequest = Request.Builder.prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(baseUri)
                        .replacePath("/api/v1/buffer/data/state")
                        .build())
                .build();

        await()
                .atMost(20, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> {
                    StringResponseHandler.StringResponse stateResponse = httpClient.execute(stateRequest, createStringResponseHandler());
                    String currentState = stateResponse.getBody().trim();
                    log.info("got current state %s for data server %s", currentState, dataServer.getNodeId());
                    return currentState.equals("ACTIVE");
                });
    }
}