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
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.starburst.stargate.buffer.discovery.client.failures.FailureInfo;
import io.starburst.stargate.buffer.discovery.client.failures.HttpFailureTrackingClient;
import io.starburst.stargate.buffer.testing.TestingBufferService;
import io.starburst.stargate.buffer.trino.exchange.fallbacking.FallbackingExchangeManager;
import io.starburst.stargate.buffer.trino.exchange.fallbacking.FallbackingExchangeStats;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.tpch.TpchTable;
import org.assertj.core.api.AssertProvider;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.testing.Closeables.closeAll;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestFallbackingExchange
{
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void test()
            throws Exception
    {
        try (TestSetup testSetup = new TestSetup();
                JettyHttpClient httpClient = new JettyHttpClient(new HttpClientConfig())) {
            ExchangeManager exchangeManager = getExchangeManager(testSetup.getQueryRunner());
            if (!(exchangeManager instanceof FallbackingExchangeManager fallbackingExchangeManager)) {
                throw new RuntimeException("Expected %s to be FallbackingExchangeManager but was %s".formatted(exchangeManager, exchangeManager.getClass()));
            }

            assertThat(testSetup.query("SELECT COUNT(*) FROM orders")).matches("SELECT BIGINT '15000'");
            assertThat(testSetup.query("SELECT COUNT(*) FROM orders")).matches("SELECT BIGINT '15000'");

            FallbackingExchangeStats.Snapshot initialStats = fallbackingExchangeManager.getStats();

            assertThat(initialStats.bufferExchangesCreated()).isGreaterThan(0);
            assertThat(initialStats.filesystemExchangesCreated()).isEqualTo(0);

            // trigger failures
            HttpFailureTrackingClient failureTrackingClient = new HttpFailureTrackingClient(testSetup.getBufferService().getDiscoveryServer().getBaseUri(), httpClient);
            for (int i = 0; i < 3; ++i) {
                failureTrackingClient.registerFailure(new FailureInfo(Optional.empty(), "some_client", "some_failure"));
            }

            assertThat(testSetup.query("SELECT COUNT(*) FROM orders")).matches("SELECT BIGINT '15000'");
            assertThat(testSetup.query("SELECT COUNT(*) FROM orders")).matches("SELECT BIGINT '15000'");

            FallbackingExchangeStats.Snapshot afterFailureStats = fallbackingExchangeManager.getStats();
            assertThat(afterFailureStats.bufferExchangesCreated()).isEqualTo(initialStats.bufferExchangesCreated());
            assertThat(afterFailureStats.filesystemExchangesCreated()).isGreaterThan(initialStats.filesystemExchangesCreated());

            Thread.sleep(1500); // we are past the decay interfval in failure tracking server but still in backoff on the exchange side
            assertThat(testSetup.query("SELECT COUNT(*) FROM orders")).matches("SELECT BIGINT '15000'");
            assertThat(testSetup.query("SELECT COUNT(*) FROM orders")).matches("SELECT BIGINT '15000'");

            FallbackingExchangeStats.Snapshot pastDecayStats = fallbackingExchangeManager.getStats();
            assertThat(pastDecayStats.bufferExchangesCreated()).isEqualTo(afterFailureStats.bufferExchangesCreated());
            assertThat(pastDecayStats.filesystemExchangesCreated()).isGreaterThan(afterFailureStats.filesystemExchangesCreated());

            Thread.sleep(1500); // we should recover to buffer service now
            assertThat(testSetup.query("SELECT COUNT(*) FROM orders")).matches("SELECT BIGINT '15000'");
            assertThat(testSetup.query("SELECT COUNT(*) FROM orders")).matches("SELECT BIGINT '15000'");

            FallbackingExchangeStats.Snapshot recoveredStats = fallbackingExchangeManager.getStats();
            assertThat(recoveredStats.bufferExchangesCreated()).isGreaterThan(pastDecayStats.bufferExchangesCreated());
            assertThat(recoveredStats.filesystemExchangesCreated()).isEqualTo(pastDecayStats.filesystemExchangesCreated());
        }
    }

    // TODO(https://github.com/trinodb/trino/pull/15140) instead
    private ExchangeManager getExchangeManager(DistributedQueryRunner queryRunner)
    {
        try {
            Field exchangeManagerRegistryField = TestingTrinoServer.class.getDeclaredField("exchangeManagerRegistry");
            exchangeManagerRegistryField.setAccessible(true);
            Object exchangeManagerRegistryObject = exchangeManagerRegistryField.get(queryRunner.getCoordinator());
            if (!(exchangeManagerRegistryObject instanceof ExchangeManagerRegistry exchangeManagerRegistry)) {
                throw new RuntimeException("expected ExchangeManagerRegistry but got " + exchangeManagerRegistryObject.getClass());
            }
            return exchangeManagerRegistry.getExchangeManager();
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static class TestSetup
            implements AutoCloseable
    {
        private final DistributedQueryRunner queryRunner;
        private final MinioStorage minioStorage;
        private final TestingBufferService bufferService;
        private final QueryAssertions assertions;

        public TestSetup()
                throws Exception
        {
            bufferService = TestingBufferService.builder()
                    .withDiscoveryServerBuilder(builder -> builder
                            .setConfigProperty("buffer.discovery.start-grace-period", "3s")
                            .setConfigProperty("buffer.failures-tracking.failures-counter-decay-duration", "1s")) // make decaying of failures info quick
                    .build();
            minioStorage = new MinioStorage("test-fallbacking-exchange-" + randomNameSuffix());
            minioStorage.start();

            ImmutableMap<String, String> bufferExchangeManagerProperties = ImmutableMap.<String, String>builder()
                    .put("buffer.exchange.buffer-discovery.uri", bufferService.getDiscoveryServer().getBaseUri().toString())
                    .put("buffer.exchange.source-handle-target-chunks-count", "4")
                    // create more granular source handles given the fault-tolerant-execution-target-task-input-size is set to lower value for testing
                    .put("buffer.exchange.source-handle-target-data-size", "1MB")
                    .buildOrThrow();

            ImmutableMap<String, String> fileSystemExchangeManagerProperties = ImmutableMap.<String, String>builder()
                    .put("filesystem.exchange.base-directories", "s3://" + minioStorage.getBucketName())
                    .put("filesystem.exchange.sink-max-file-size", "16MB")
                    .put("filesystem.exchange.s3.aws-access-key", MinioStorage.ACCESS_KEY)
                    .put("filesystem.exchange.s3.aws-secret-key", MinioStorage.SECRET_KEY)
                    .put("filesystem.exchange.s3.region", "us-east-1")
                    .put("filesystem.exchange.s3.endpoint", "http://" + minioStorage.getMinio().getMinioApiEndpoint())
                    .buildOrThrow();

            ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                    .putAll(bufferExchangeManagerProperties)
                    .putAll(fileSystemExchangeManagerProperties)
                    .put("exchange.failure-tracking.uri", bufferService.getDiscoveryServer().getBaseUri().toString())
                    .put("exchange.fallback-recent-failures-count-threshold", "2")
                    .put("exchange.min-fallback-duration", "3s")
                    .put("exchange.max-fallback-duration", "3s")
                    .buildOrThrow();

            Map<String, String> extraProperties = new HashMap<>();
            extraProperties.putAll(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
            extraProperties.put("fault-tolerant-execution-partition-count", "50"); // use more partition to be sure we get some on the drained node
            extraProperties.put("optimizer.join-reordering-strategy", "NONE");
            extraProperties.put("join-distribution-type", "PARTITIONED");
            extraProperties.put("enable-dynamic-filtering", "false");

            queryRunner = MemoryQueryRunner.builder()
                    .setNodeCount(1)
                    .setExtraProperties(extraProperties)
                    .setAdditionalSetup(runner -> {
                        runner.installPlugin(new io.starburst.stargate.buffer.trino.exchange.fallbacking.FallbackingExchangePlugin());
                        runner.loadExchangeManager("bufferwithfallback", exchangeManagerProperties);
                    })
                    .setInitialTables(Set.of(TpchTable.ORDERS))
                    .build();

            assertions = new QueryAssertions(queryRunner);
        }

        public AssertProvider<QueryAssertions.QueryAssert> query(String query)
        {
            return assertions.query(query);
        }

        public TestingBufferService getBufferService()
        {
            return bufferService;
        }

        public DistributedQueryRunner getQueryRunner()
        {
            return queryRunner;
        }

        @Override
        public void close()
                throws Exception
        {
            closeAll(
                    assertions,
                    queryRunner,
                    bufferService,
                    minioStorage);
        }
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        executor.shutdownNow();
    }
}
