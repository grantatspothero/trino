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
import io.trino.metadata.InternalNode;
import io.trino.plugin.memory.MemoryCacheManager.SplitKey;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.cache.CacheManager.PreferredAddressProvider;
import io.trino.spi.cache.CacheManager.SplitCache;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.cache.SplitId;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingNodeManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.memory.MemoryCacheManager.MAP_ENTRY_SIZE;
import static io.trino.plugin.memory.MemoryCacheManager.SETTABLE_FUTURE_INSTANCE_SIZE;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestMemoryCacheManager
{
    private MemoryCacheManager cacheManager;
    private long allocatedRevocableMemory;
    private long memoryLimit;

    @BeforeMethod
    public void setup()
    {
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
        SplitId splitId = new SplitId("split1");

        // split data should not be cached yet
        SplitCache cache = cacheManager.getSplitCache(signature);
        assertThat(cache.loadPages(splitId)).isEmpty();

        Optional<ConnectorPageSink> sinkOptional = cache.storePages(splitId);
        assertThat(sinkOptional).isPresent();

        // second sink should not be present as split data is already being cached
        assertThat(cache.storePages(splitId)).isEmpty();
        assertThat(cache.loadPages(splitId)).isEmpty();

        Page page = createOneMegaBytePage();
        ConnectorPageSink sink = sinkOptional.get();
        sink.appendPage(page);

        // make sure memory usage is accounted for in page sink
        assertThat(sink.getMemoryUsage()).isEqualTo(page.getRetainedSizeInBytes());
        assertThat(allocatedRevocableMemory).isEqualTo(0);

        // make sure memory is transferred to cacheManager after sink is finished
        sink.finish();
        long cacheEntrySize = 2L * (MAP_ENTRY_SIZE + SplitKey.INSTANCE_SIZE + splitId.getRetainedSizeInBytes()) + SETTABLE_FUTURE_INSTANCE_SIZE;
        assertThat(allocatedRevocableMemory).isEqualTo(page.getRetainedSizeInBytes() + cacheEntrySize);

        // split data should be available now
        Optional<ConnectorPageSource> sourceOptional = cache.loadPages(splitId);
        assertThat(sourceOptional).isPresent();

        // ensure cached pages are correct
        ConnectorPageSource source = sourceOptional.get();
        assertThat(source.getMemoryUsage()).isEqualTo(page.getRetainedSizeInBytes());
        assertThat(source.getNextPage()).isEqualTo(page);
        assertThat(source.isFinished()).isTrue();

        // make sure no data is available for other signatures
        PlanSignature signature2 = createPlanSignature("sig2");
        SplitCache cache2 = cacheManager.getSplitCache(signature2);
        assertThat(cache2.loadPages(splitId)).isEmpty();
        cache2.close();

        // store data for another split
        SplitId splitId2 = new SplitId("split2");
        sink = cache.storePages(splitId2).get();
        sink.appendPage(page);
        sink.finish();
        assertThat(allocatedRevocableMemory).isEqualTo(2 * (page.getRetainedSizeInBytes() + cacheEntrySize));

        // data for both splits should be cached
        assertThat(cache.loadPages(splitId)).isPresent();
        assertThat(cache.loadPages(splitId2)).isPresent();

        // revoke memory and make sure only the least recently used split is left
        cacheManager.revokeMemory(1_500_000);
        assertThat(allocatedRevocableMemory).isEqualTo(page.getRetainedSizeInBytes() + cacheEntrySize);
        assertThat(cache.loadPages(splitId)).isEmpty();

        // make sure no new split data is cached when memory limit is lowered
        memoryLimit = 1_500_000;
        sink = cache.storePages(splitId).get();
        sink.appendPage(page);
        sink.finish();
        assertThat(allocatedRevocableMemory).isEqualTo(page.getRetainedSizeInBytes() + cacheEntrySize);
        assertThat(cache.loadPages(splitId)).isEmpty();

        cache.close();
    }

    @Test
    public void testSinkAbort()
            throws IOException
    {
        PlanSignature signature = createPlanSignature("sig");
        SplitId splitId = new SplitId("split");

        // start caching of split data
        SplitCache cache = cacheManager.getSplitCache(signature);
        Optional<ConnectorPageSink> sinkOptional = cache.storePages(splitId);
        assertThat(sinkOptional).isPresent();
        ConnectorPageSink sink = sinkOptional.get();
        sink.appendPage(createOneMegaBytePage());
        assertThat(allocatedRevocableMemory).isEqualTo(0);
        assertThat(cache.loadPages(splitId)).isEmpty();

        // no data should be cached after abort
        sink.abort();
        assertThat(allocatedRevocableMemory).isEqualTo(0);
        assertThat(cache.loadPages(splitId)).isEmpty();

        cache.close();
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
        SplitId splitId1 = new SplitId("split1");
        SplitId splitId2 = new SplitId("split2");
        PreferredAddressProvider addressProvider1 = cacheManager.getPreferredAddressProvider(signature1, nodeManager);
        PreferredAddressProvider addressProvider2 = cacheManager.getPreferredAddressProvider(signature2, nodeManager);

        // assert that both different signature or split id affects preferred address
        assertThat(addressProvider1.getPreferredAddress(splitId1)).isNotEqualTo(addressProvider1.getPreferredAddress(splitId2));
        assertThat(addressProvider2.getPreferredAddress(splitId1)).isNotEqualTo(addressProvider2.getPreferredAddress(splitId2));
        assertThat(addressProvider1.getPreferredAddress(splitId1)).isNotEqualTo(addressProvider2.getPreferredAddress(splitId1));
    }

    private static PlanSignature createPlanSignature(String signature)
    {
        return new PlanSignature(
                new SignatureKey(signature),
                Optional.empty(),
                ImmutableList.of(),
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
