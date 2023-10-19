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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.trino.client.NodeVersion;
import io.trino.plugin.memory.MemoryCacheManager.SplitKey;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.cache.CacheManager.PreferredAddressProvider;
import io.trino.spi.cache.CacheManager.SplitCache;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.memory.EmptySplitCache.EMPTY_SPLIT_CACHE;
import static io.trino.plugin.memory.MemoryCacheManager.MAP_ENTRY_SIZE;
import static io.trino.plugin.memory.MemoryCacheManager.SETTABLE_FUTURE_INSTANCE_SIZE;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestMemoryCacheManager
{
    private Page oneMegabytePage;
    private MemoryCacheManager cacheManager;
    private long allocatedRevocableMemory;
    private long memoryLimit;

    @BeforeMethod
    public void setup()
    {
        oneMegabytePage = createOneMegaBytePage();
        allocatedRevocableMemory = 0;
        memoryLimit = Long.MAX_VALUE;
        CacheManagerContext context = () -> bytes -> {
            checkArgument(bytes >= 0);
            if (bytes > memoryLimit) {
                return false;
            }
            allocatedRevocableMemory = bytes;
            return true;
        };
        cacheManager = new MemoryCacheManager(context);
    }

    @Test
    public void testCachePages()
            throws IOException
    {
        PlanSignature signature = createPlanSignature("sig");
        CacheSplitId splitId = new CacheSplitId("split1");

        // split data should not be cached yet
        SplitCache cache = cacheManager.getSplitCache(signature);
        assertThat(cache.loadPages(splitId)).isEmpty();
        long signatureIdSize = ObjectToIdMap.getEntrySize(signature, PlanSignature::getRetainedSizeInBytes);
        assertThat(allocatedRevocableMemory).isEqualTo(signatureIdSize);

        Optional<ConnectorPageSink> sinkOptional = cache.storePages(splitId);
        assertThat(sinkOptional).isPresent();

        // second sink should not be present as split data is already being cached
        assertThat(cache.storePages(splitId)).isEmpty();
        assertThat(cache.loadPages(splitId)).isEmpty();

        ConnectorPageSink sink = sinkOptional.get();
        sink.appendPage(oneMegabytePage);

        // make sure memory usage is accounted for in page sink
        assertThat(sink.getMemoryUsage()).isEqualTo(oneMegabytePage.getRetainedSizeInBytes());
        assertThat(allocatedRevocableMemory).isEqualTo(signatureIdSize);

        // make sure memory is transferred to cacheManager after sink is finished
        sink.finish();
        long cacheEntrySize = 2L * MAP_ENTRY_SIZE + SplitKey.INSTANCE_SIZE + splitId.getRetainedSizeInBytes() + SETTABLE_FUTURE_INSTANCE_SIZE;
        assertThat(allocatedRevocableMemory).isEqualTo(oneMegabytePage.getRetainedSizeInBytes() + cacheEntrySize + signatureIdSize);

        // split data should be available now
        Optional<ConnectorPageSource> sourceOptional = cache.loadPages(splitId);
        assertThat(sourceOptional).isPresent();

        // ensure cached pages are correct
        ConnectorPageSource source = sourceOptional.get();
        assertThat(source.getMemoryUsage()).isEqualTo(oneMegabytePage.getRetainedSizeInBytes());
        assertThat(source.getNextPage()).isEqualTo(oneMegabytePage);
        assertThat(source.isFinished()).isTrue();

        // make sure no data is available for other signatures
        PlanSignature anotherSignature = createPlanSignature("sig2");
        SplitCache anotherCache = cacheManager.getSplitCache(anotherSignature);
        assertThat(anotherCache.loadPages(splitId)).isEmpty();
        long anotherSignatureIdSize = ObjectToIdMap.getEntrySize(anotherSignature, PlanSignature::getRetainedSizeInBytes);
        assertThat(allocatedRevocableMemory).isEqualTo(oneMegabytePage.getRetainedSizeInBytes() + cacheEntrySize + signatureIdSize + anotherSignatureIdSize);
        anotherCache.close();
        // SplitCache close should release signature memory
        assertThat(allocatedRevocableMemory).isEqualTo(oneMegabytePage.getRetainedSizeInBytes() + cacheEntrySize + signatureIdSize);

        // store data for another split
        CacheSplitId splitId2 = new CacheSplitId("split2");
        sink = cache.storePages(splitId2).orElseThrow();
        sink.appendPage(oneMegabytePage);
        sink.finish();
        assertThat(allocatedRevocableMemory).isEqualTo(2 * (oneMegabytePage.getRetainedSizeInBytes() + cacheEntrySize) + signatureIdSize);

        // data for both splits should be cached
        assertThat(cache.loadPages(splitId)).isPresent();
        assertThat(cache.loadPages(splitId2)).isPresent();

        // revoke memory and make sure only the least recently used split is left
        assertThat(cacheManager.getRevokeMemoryTime().getAllTime().getCount()).isZero();
        cacheManager.revokeMemory(500_000);
        assertThat(allocatedRevocableMemory).isEqualTo(oneMegabytePage.getRetainedSizeInBytes() + cacheEntrySize + signatureIdSize);
        assertThat(cache.loadPages(splitId)).isEmpty();
        assertThat(cacheManager.getRevokeMemoryTime().getAllTime().getCount()).isNotZero();

        // make sure no new split data is cached when memory limit is lowered
        memoryLimit = 1_500_000;
        sink = cache.storePages(splitId).orElseThrow();
        sink.appendPage(oneMegabytePage);
        sink.finish();
        assertThat(allocatedRevocableMemory).isEqualTo(oneMegabytePage.getRetainedSizeInBytes() + cacheEntrySize + signatureIdSize);
        assertThat(cache.loadPages(splitId)).isEmpty();

        cache.close();
    }

    @Test
    public void testSinkAbort()
            throws IOException
    {
        PlanSignature signature = createPlanSignature("sig");
        CacheSplitId splitId = new CacheSplitId("split");

        // create new SplitCache
        long signatureIdSize = ObjectToIdMap.getEntrySize(signature, PlanSignature::getRetainedSizeInBytes);
        SplitCache cache = cacheManager.getSplitCache(signature);

        // start caching of new split
        Optional<ConnectorPageSink> sinkOptional = cache.storePages(splitId);
        assertThat(sinkOptional).isPresent();
        ConnectorPageSink sink = sinkOptional.get();
        sink.appendPage(oneMegabytePage);
        assertThat(allocatedRevocableMemory).isEqualTo(signatureIdSize);
        assertThat(sink.getMemoryUsage()).isEqualTo(oneMegabytePage.getRetainedSizeInBytes());
        assertThat(cache.loadPages(splitId)).isEmpty();

        // active sink should keep signature memory allocated
        cache.close();
        assertThat(allocatedRevocableMemory).isEqualTo(signatureIdSize);

        // no data should be cached after abort
        sink.abort();
        assertThat(allocatedRevocableMemory).isEqualTo(0L);
        assertThat(cacheManager.getSplitCache(signature).loadPages(splitId)).isEmpty();
    }

    @Test
    public void testPlanSignatureRevoke()
            throws IOException
    {
        Page smallPage = new Page(new IntArrayBlock(1, Optional.empty(), new int[] {0}));
        PlanSignature bigSignature = createPlanSignature(IntStream.range(0, 500_000).mapToObj(Integer::toString).collect(joining()));
        PlanSignature secondBigSignature = createPlanSignature(IntStream.range(0, 500_001).mapToObj(Integer::toString).collect(joining()));

        // cache some data for first signature
        CacheSplitId splitId = new CacheSplitId("split");
        assertThat(allocatedRevocableMemory).isEqualTo(0);
        SplitCache cache = cacheManager.getSplitCache(bigSignature);
        ConnectorPageSink sink = cache.storePages(splitId).orElseThrow();
        sink.appendPage(smallPage);
        sink.finish();
        cache.close();

        // make sure page is present with new SplitCache instance
        SplitCache anotherCache = cacheManager.getSplitCache(bigSignature);
        assertThat(anotherCache.loadPages(splitId)).isPresent();

        // cache data for another signature
        SplitCache cacheForSecondSignature = cacheManager.getSplitCache(secondBigSignature);
        sink = cacheForSecondSignature.storePages(splitId).orElseThrow();
        sink.appendPage(smallPage);
        sink.finish();

        // both splits should be still cached
        assertThat(anotherCache.loadPages(splitId)).isPresent();
        assertThat(cacheForSecondSignature.loadPages(splitId)).isPresent();
        anotherCache.close();
        cacheForSecondSignature.close();

        // revoke some small amount of memory
        assertThat(cacheManager.revokeMemory(100)).isPositive();

        // only one split (for secondBigSignature signature) should be cached, because big signature was purged
        anotherCache = cacheManager.getSplitCache(bigSignature);
        cacheForSecondSignature = cacheManager.getSplitCache(secondBigSignature);
        assertThat(anotherCache.loadPages(splitId)).isEmpty();
        assertThat(cacheForSecondSignature.loadPages(splitId)).isPresent();
        anotherCache.close();

        // memory limits should be enforced for large signatures
        memoryLimit = 10_000;
        long initialMemory = allocatedRevocableMemory;
        SplitCache thirdCache = cacheManager.getSplitCache(bigSignature);
        assertThat(thirdCache).isEqualTo(EMPTY_SPLIT_CACHE);
        assertThat(allocatedRevocableMemory).isEqualTo(initialMemory);
    }

    @Test
    public void testAddressProvider()
            throws URISyntaxException
    {
        TestingNodeManager nodeManager = new TestingNodeManager();
        nodeManager.addNode(new InternalNode("node1", new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN, false));
        nodeManager.addNode(new InternalNode("node2", new URI("http://127.0.0.2/"), NodeVersion.UNKNOWN, false));
        nodeManager.addNode(new InternalNode("node3", new URI("http://127.0.0.3/"), NodeVersion.UNKNOWN, false));
        nodeManager.addNode(new InternalNode("node4", new URI("http://127.0.0.4/"), NodeVersion.UNKNOWN, false));

        PlanSignature signature1 = createPlanSignature("sig1");
        PlanSignature signature2 = createPlanSignature("sig2");
        CacheSplitId splitId1 = new CacheSplitId("split1");
        CacheSplitId splitId2 = new CacheSplitId("split2");
        PreferredAddressProvider addressProvider1 = cacheManager.getPreferredAddressProvider(signature1, nodeManager);
        PreferredAddressProvider addressProvider2 = cacheManager.getPreferredAddressProvider(signature2, nodeManager);

        // assert that both different signature or split id affects preferred address
        assertThat(addressProvider1.getPreferredAddress(splitId1)).isNotEqualTo(addressProvider1.getPreferredAddress(splitId2));
        assertThat(addressProvider2.getPreferredAddress(splitId1)).isNotEqualTo(addressProvider2.getPreferredAddress(splitId2));
        assertThat(addressProvider1.getPreferredAddress(splitId1)).isNotEqualTo(addressProvider2.getPreferredAddress(splitId1));
    }

    @Test
    public void testSplitSizeDistributionMetric()
    {
        PlanSignature signature = createPlanSignature("sig");
        CacheSplitId splitId = new CacheSplitId("split1");
        SplitCache cache = cacheManager.getSplitCache(signature);
        assertThat(cache.loadPages(splitId)).isEmpty();

        Optional<ConnectorPageSink> sink = cache.storePages(splitId);
        assertThat(sink).isPresent();

        sink.get().appendPage(oneMegabytePage);
        sink.get().finish();

        Map<Double, Double> splitSizePercentiles = cacheManager.getCachedSplitSizeDistribution();
        assertThat(splitSizePercentiles.get(0.01)).isEqualTo(oneMegabytePage.getRetainedSizeInBytes());
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

    private static Page createOneMegaBytePage()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(0);
        while (blockBuilder.getRetainedSizeInBytes() < 1024 * 1024) {
            BIGINT.writeLong(blockBuilder, 42L);
        }
        return new Page(0, blockBuilder.build());
    }
}
