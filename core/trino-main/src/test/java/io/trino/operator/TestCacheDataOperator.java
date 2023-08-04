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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.cache.CacheConfig;
import io.trino.cache.CacheDataOperator;
import io.trino.cache.CacheDriverContext;
import io.trino.cache.CacheManagerRegistry;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.NodeMemoryConfig;
import io.trino.spi.Page;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.PlanNodeIdAllocator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.Executors;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.operator.PageTestUtils.createPage;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestCacheDataOperator
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private CacheManagerRegistry registry;

    @BeforeMethod
    public void setUp()
    {
        NodeMemoryConfig config = new NodeMemoryConfig()
                .setHeapHeadroom(DataSize.of(10, MEGABYTE))
                .setMaxQueryMemoryPerNode(DataSize.of(100, MEGABYTE));
        LocalMemoryManager memoryManager = new LocalMemoryManager(config, DataSize.of(110, MEGABYTE).toBytes());
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setEnabled(true);
        registry = new CacheManagerRegistry(cacheConfig, memoryManager);
        registry.loadCacheManager();
    }

    @Test
    public void testLimitCache()
    {
        PlanSignature signature = createPlanSignature("sig");
        CacheSplitId splitId = new CacheSplitId("split");
        CacheManager.SplitCache splitCache = registry.getCacheManager().getSplitCache(signature);
        Optional<ConnectorPageSink> sink = splitCache.storePages(splitId);
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();

        CacheDataOperator.CacheDataOperatorFactory operatorFactory = new CacheDataOperator.CacheDataOperatorFactory(
                0,
                planNodeIdAllocator.getNextId(),
                DataSize.of(1024, DataSize.Unit.BYTE).toBytes());
        DriverContext driverContext = createTaskContext(Executors.newSingleThreadExecutor(), Executors.newScheduledThreadPool(1), TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        driverContext.setCacheDriverContext(new CacheDriverContext(Optional.empty(), sink, EMPTY));
        CacheDataOperator cacheDataOperator = (CacheDataOperator) operatorFactory.createOperator(driverContext);

        // sink was not aborted - there is a space in a cache. The page was passed through and split is going to be cached
        Page smallPage = createPage(ImmutableList.of(BIGINT), 1, Optional.empty(), ImmutableList.of(createLongSequenceBlock(0, 10)));
        cacheDataOperator.addInput(smallPage);
        assertThat(cacheDataOperator.getOutput()).isEqualTo(smallPage);
        assertThat(sink.get().getMemoryUsage()).isEqualTo(204L);
        cacheDataOperator.finish();

        // sink was aborted - there is no sufficient space in a cache. The page was passed through but split is not going to be cached
        Page bigPage = createPage(ImmutableList.of(BIGINT), 1, Optional.empty(), ImmutableList.of(createLongSequenceBlock(0, 102)));
        driverContext = createTaskContext(Executors.newSingleThreadExecutor(), Executors.newScheduledThreadPool(1), TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        driverContext.setCacheDriverContext(new CacheDriverContext(Optional.empty(), sink, EMPTY));
        cacheDataOperator = (CacheDataOperator) operatorFactory.createOperator(driverContext);

        cacheDataOperator.addInput(bigPage);
        cacheDataOperator.finish();

        assertThat(cacheDataOperator.getOutput()).isEqualTo(bigPage);
        assertThat(sink.get().getMemoryUsage()).isEqualTo(1144L);
    }

    private static PlanSignature createPlanSignature(String signature)
    {
        return new PlanSignature(
                new SignatureKey(signature),
                Optional.empty(),
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all());
    }
}
