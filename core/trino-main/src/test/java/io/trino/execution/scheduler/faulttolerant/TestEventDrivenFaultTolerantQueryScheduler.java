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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.testing.TestingBufferService;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangePlugin;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.client.ClientCapabilities;
import io.trino.client.NodeVersion;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.StageId;
import io.trino.execution.TestingRemoteTaskFactory;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.execution.scheduler.TaskExecutionStats;
import io.trino.execution.warnings.WarningCollector;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.LanguageFunctionManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProperties;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.security.AllowAllAccessControl;
import io.trino.server.DynamicFilterService;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.planner.AdaptivePlanner;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.testing.TestingConnectorContext;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Arrays.stream;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestEventDrivenFaultTolerantQueryScheduler
        extends BasePlanTest
{
    private static final ConnectorName testCatalogConnector = new ConnectorName("test_catalog_connector");

    public TestEventDrivenFaultTolerantQueryScheduler()
    {
        super(ImmutableMap.of());
    }

    @Test
    @Disabled // TODO[https://github.com/starburstdata/galaxy-trino/issues/1924] make compatible with AdaptivePlanner changes
    void testSpeculativeStageExecutionCreation()
            throws IOException
    {
        try (TestingTrinoServer server = TestingTrinoServer.create()) {
            server.installPlugin(new TpchPlugin()); // Makes server.createCatalog() safe to run.
            loadBufferExchangePlugin(server);
            server.createCatalog(TEST_CATALOG_NAME, "tpch"); // Must happen before QueryStateMachine creation.
            ConnectorContext context = new TestingConnectorContext();
            String query = "SELECT * FROM (SELECT name, partkey, brand FROM part ORDER BY size) WHERE brand='acme' LIMIT 999";
            Session session = makeSession(server);
            TransactionManager transactionManager = server.getTransactionManager();
            MockMetadata metadata = new MockMetadata(server.getInstance(Key.get(LanguageFunctionManager.class)));
            ScheduledExecutorService executorService = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
            QueryStateMachine queryStateMachine = QueryStateMachine.beginWithTicker(
                    Optional.empty(),
                    query,
                    Optional.empty(),
                    session,
                    URI.create("fake://fake-query"),
                    new ResourceGroupId("test"),
                    false,
                    transactionManager,
                    new AllowAllAccessControl(),
                    executorService,
                    Ticker.systemTicker(),
                    metadata,
                    WarningCollector.NOOP,
                    createPlanOptimizersStatsCollector(),
                    Optional.of(QueryType.SELECT),
                    false,
                    new NodeVersion("test"));
            PlanFragmenter planFragmenter = server.getInstance(Key.get(PlanFragmenter.class));
            SubPlan subplan = planFragmenter.createSubPlans(queryStateMachine.getSession(), plan(query, OPTIMIZED, false), false, queryStateMachine.getWarningCollector());
            assertThat(subplan.getChildren().size()).isGreaterThan(0);  // Need multiple stages to meaningfully test that latter stages are marked speculative.

            AdaptivePlanner adaptivePlanner = null;
//            AdaptivePlanner adaptivePlanner = new AdaptivePlanner(
//                    session,
//                    getPlannerContext(),
//                    optimizers,
//                    planFragmenter,
//                    new PlanSanityChecker(false),
//                    new IrTypeAnalyzer(plannerContext),
//                    warningCollector,
//                    planOptimizersStatsCollector,
//                    new CachingTableStatsProvider(metadata, session));

            EventDrivenFaultTolerantQueryScheduler.Scheduler scheduler = new EventDrivenFaultTolerantQueryScheduler.Scheduler(
                    queryStateMachine,
                    metadata,
                    new TestingRemoteTaskFactory(),
                    new TaskDescriptorStorage(DataSize.of(5, KILOBYTE), jsonCodec(Split.class)),
                    server.getInstance(Key.get(EventDrivenTaskSourceFactory.class)),
                    false,
                    server.getInstance(Key.get(NodeTaskMap.class)),
                    newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s")),
                    executorService,
                    context.getTracer(),
                    new SplitSchedulerStats(),
                    server.getInstance(Key.get(PartitionMemoryEstimatorFactory.class)),
                    server.getInstance(Key.get(OutputStatsEstimatorFactory.class)).create(session),
                    new FaultTolerantPartitioningSchemeFactory(
                            server.getNodePartitioningManager(),
                            session,
                            5),
                    server.getExchangeManager(),
                    3,
                    3,
                    5,
                    new BinPackingNodeAllocatorService(
                            new InMemoryNodeManager(new InternalNode("node-1", URI.create("local://127.0.0.1:10"), NodeVersion.UNKNOWN, false)),
                            () -> null,
                            false,
                            Duration.of(1, MINUTES),
                            DataSize.of(4, GIGABYTE),
                            DataSize.of(10, GIGABYTE),
                            Ticker.systemTicker()),
                    server.getInstance(Key.get(FailureDetector.class)),
                    new EventDrivenFaultTolerantQueryScheduler.StageRegistry(queryStateMachine, subplan),
                    server.getInstance(Key.get(TaskExecutionStats.class)),
                    server.getInstance(Key.get(DynamicFilterService.class)),
                    new EventDrivenFaultTolerantQueryScheduler.SchedulingDelayer(
                            io.airlift.units.Duration.valueOf("2s"),
                            io.airlift.units.Duration.valueOf("20s"),
                            1.3,
                            Stopwatch.createUnstarted()),
                    subplan,
                    100,
                    DataSize.of(1, GIGABYTE),
                    2,
                    false,
                    0,
                    DataSize.of(5, GIGABYTE),
                    true,
                    adaptivePlanner,
                    true);
            StageId stageId = scheduler.getStageId(subplan.getFragment().getId());
            assertThatThrownBy(() -> scheduler.getStageExecution(stageId)).hasMessageContaining("stage execution does not exist");
            scheduler.schedule();
            assertThat(scheduler.getStageExecution(stageId).isSpeculative()).isTrue();
        }
    }

    private static Session makeSession(TestingTrinoServer server)
    {
        // Wish we could just use the SessionPropertyManager injected into the TestingTrinoServer, but IDK how to add connector-property definitions to it.
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(
                ImmutableSet.of(server.getInstance(Key.get(SystemSessionProperties.class))),
                // Later we'll set a catalog session property, or the catalog won't get registered in the TransactionManager during Session.beginTransactionId().  But it must be a
                // valid property, recognized by this SessionPropertyManager.
                catalogHandle -> ImmutableMap.of("p", PropertyMetadata.booleanProperty("p", ".", false, false)));

        return testSessionBuilder(sessionPropertyManager)
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .setClientCapabilities(stream(ClientCapabilities.values())
                        .map(ClientCapabilities::toString)
                        .collect(toImmutableSet()))
                .setCatalogSessionProperty(TEST_CATALOG_NAME, "p", "false")
                .setSystemProperty(RETRY_POLICY, "TASK")
                .build();
    }

    private void loadBufferExchangePlugin(TestingTrinoServer server)
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
                                .setConfigProperty("chunk.spool-merge-enabled", "true")
                                .setConfigProperty("exchange.staleness-threshold", "2h"))
                .setDataServersCount(3)
                .build();
        server.installPlugin(new BufferExchangePlugin());
        URI discoveryServerUri = bufferService.getDiscoveryServer().getBaseUri();
        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.buffer-discovery.uri", discoveryServerUri.toString())
                .put("exchange.sink-target-written-pages-count", "3")
                .put("exchange.sink-blocked-memory-low", "32MB")
                .put("exchange.sink-blocked-memory-high", "64MB")
                .put("exchange.source-handle-target-chunks-count", "4")
                .put("exchange.min-base-buffer-nodes-per-partition", "2")
                .put("exchange.max-base-buffer-nodes-per-partition", "2")
                .put("exchange.buffer-data.spooling-storage-type", "LOCAL")
                .buildOrThrow();
        server.loadExchangeManager("buffer", exchangeManagerProperties);
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final LanguageFunctionManager languageFunctionManager;

        private MockMetadata(LanguageFunctionManager languageFunctionManager)
        {
            this.languageFunctionManager = languageFunctionManager;
        }

        @Override
        public CatalogSchemaTableName getTableName(Session session, TableHandle tableHandle)
        {
            String[] schemaAndTable = tableHandle.getConnectorHandle().toString().split(":");
            return new CatalogSchemaTableName(tableHandle.getCatalogHandle().getCatalogName().toString(), new SchemaTableName(schemaAndTable[0], schemaAndTable[1]));
        }

        @Override
        public TableProperties getTableProperties(Session session, TableHandle handle)
        {
            return new TableProperties(TEST_CATALOG_HANDLE, TestingConnectorTransactionHandle.INSTANCE, new ConnectorTableProperties());
        }

        @Override
        public void beginQuery(Session session)
        {
            languageFunctionManager.registerQuery(session);
        }

        @Override
        public List<CatalogInfo> listCatalogs(Session session)
        {
            return List.of(new CatalogInfo(TEST_CATALOG_NAME, TEST_CATALOG_HANDLE, testCatalogConnector));
        }
    }
}
