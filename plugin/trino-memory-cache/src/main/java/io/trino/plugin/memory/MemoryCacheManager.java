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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.stats.Distribution;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.MemoryAllocator;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.FixedPageSource;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.consistentHash;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.memory.EmptySplitCache.EMPTY_SPLIT_CACHE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * {@link CacheManager} implementation that caches split pages in revocable memory and does not have
 * support for any adaption (column pruning, filtering).
 */
public class MemoryCacheManager
        implements CacheManager
{
    // based on SizeOf.estimatedSizeOf(java.util.Map<K,V>, java.util.function.ToLongFunction<K>, java.util.function.ToLongFunction<V>)
    static final int MAP_ENTRY_SIZE = instanceSize(AbstractMap.SimpleEntry.class);
    static final int SETTABLE_FUTURE_INSTANCE_SIZE = instanceSize(SettableFuture.class);

    private static final long WORKER_NODES_CACHE_TIMEOUT_SECS = 10;

    private final MemoryAllocator revocableMemoryAllocator;

    @GuardedBy("this")
    private final Map<SplitKey, List<Page>> splitCache = new LinkedHashMap<>();
    @GuardedBy("this")
    private final Map<SplitKey, SettableFuture<?>> splitLoaded = new HashMap<>();
    @GuardedBy("this")
    private final ObjectToIdMap<PlanSignature> signatureToId = new ObjectToIdMap<>(PlanSignature::getRetainedSizeInBytes);
    private final Distribution cachedSplitSizeDistribution = new Distribution();
    @GuardedBy("this")
    private long cacheRevocableBytes;

    @Inject
    public MemoryCacheManager(CacheManagerContext context)
    {
        requireNonNull(context, "context is null");
        this.revocableMemoryAllocator = context.revocableMemoryAllocator();
    }

    @Override
    public SplitCache getSplitCache(PlanSignature signature)
    {
        return allocateSignatureId(signature)
                .<SplitCache>map(MemorySplitCache::new)
                .orElse(EMPTY_SPLIT_CACHE);
    }

    @Override
    public PreferredAddressProvider getPreferredAddressProvider(PlanSignature signature, NodeManager nodeManager)
    {
        return new MemoryPreferredAddressProvider(signature, nodeManager);
    }

    @Override
    public synchronized long revokeMemory(long bytesToRevoke)
    {
        checkArgument(bytesToRevoke >= 0);
        long initialRevocableBytes = getRevocableBytes();
        return removeEldestSplits(() -> initialRevocableBytes - getRevocableBytes() >= bytesToRevoke);
    }

    @Managed
    @Nested
    public Distribution getCachedSplitSizeDistribution()
    {
        return cachedSplitSizeDistribution;
    }

    @Managed
    public synchronized long getRevocableBytes()
    {
        return cacheRevocableBytes + signatureToId.getRevocableBytes();
    }

    @Managed
    public synchronized int getCachedPlanSignaturesCount()
    {
        return signatureToId.size();
    }

    private synchronized Optional<ConnectorPageSource> loadPages(long signatureId, CacheSplitId splitId)
    {
        SplitKey key = new SplitKey(signatureId, splitId);
        ListenableFuture<?> loaded = splitLoaded.get(key);
        if (loaded == null || !loaded.isDone()) {
            // split not cached
            return Optional.empty();
        }

        // make entry the freshest in cache
        List<Page> pages = requireNonNull(splitCache.remove(key));
        splitCache.put(key, pages);

        return Optional.of(new FixedPageSource(pages));
    }

    private synchronized Optional<ConnectorPageSink> storePages(long signatureId, CacheSplitId splitId)
    {
        SplitKey key = new SplitKey(signatureId, splitId);
        SettableFuture<?> loaded = splitLoaded.get(key);
        if (loaded != null) {
            // split is already cached or currently being stored
            return Optional.empty();
        }

        signatureToId.acquireId(signatureId);
        // memory for splitLoaded entry will be accounted in finishStorePages
        splitLoaded.put(key, SettableFuture.create());
        return Optional.of(new MemoryCachePageSink(key));
    }

    private synchronized void finishStorePages(SplitKey key, long memoryUsageBytes, List<Page> pages)
    {
        long entrySize = getCacheEntrySize(key, memoryUsageBytes);
        if (!revocableMemoryAllocator.trySetBytes(getRevocableBytes() + entrySize)) {
            // not sufficient memory to store split pages
            abortStorePages(key);
            return;
        }

        cacheRevocableBytes += entrySize;
        splitCache.put(key, pages);
        splitLoaded.get(key).set(null);
        cachedSplitSizeDistribution.add(memoryUsageBytes);
        checkState(signatureToId.getUsageCount(key.signatureId()) > 0, "Signature id must not be released while split is cached");
    }

    private synchronized void abortStorePages(SplitKey key)
    {
        splitLoaded.remove(key).set(null);
        releaseSignatureId(key.signatureId());
    }

    private synchronized long removeEldestSplits(BooleanSupplier stopCondition)
    {
        if (splitCache.isEmpty()) {
            // no splits to remove
            return 0L;
        }

        long initialRevocableBytes = getRevocableBytes();
        for (Iterator<Map.Entry<SplitKey, List<Page>>> iterator = splitCache.entrySet().iterator(); iterator.hasNext(); ) {
            if (stopCondition.getAsBoolean()) {
                break;
            }

            Map.Entry<SplitKey, List<Page>> entry = iterator.next();
            SplitKey key = entry.getKey();
            long entrySize = getCacheEntrySize(key, entry.getValue());

            iterator.remove();
            splitLoaded.remove(key);
            signatureToId.releaseId(key.signatureId());

            cacheRevocableBytes -= entrySize;
        }
        checkState(cacheRevocableBytes >= 0);

        // freeing memory should always succeed, while any non-negative allocation might return false
        long currentRevocableBytes = getRevocableBytes();
        checkState(initialRevocableBytes >= currentRevocableBytes);
        checkState(initialRevocableBytes == currentRevocableBytes || revocableMemoryAllocator.trySetBytes(currentRevocableBytes));
        return initialRevocableBytes - currentRevocableBytes;
    }

    private synchronized Optional<Long> allocateSignatureId(PlanSignature signature)
    {
        long initialRevocableBytes = getRevocableBytes();

        long signatureId = signatureToId.allocateId(signature);

        long currentRevocableBytes = getRevocableBytes();
        checkState(currentRevocableBytes >= initialRevocableBytes);
        if (currentRevocableBytes > initialRevocableBytes && !revocableMemoryAllocator.trySetBytes(currentRevocableBytes)) {
            // couldn't allocate ids due to memory constraints
            signatureToId.releaseId(signatureId);
            return Optional.empty();
        }

        return Optional.of(signatureId);
    }

    private synchronized void releaseSignatureId(long signatureId)
    {
        long initialRevocableBytes = getRevocableBytes();
        signatureToId.releaseId(signatureId);
        long currentRevocableBytes = getRevocableBytes();
        checkState(initialRevocableBytes >= currentRevocableBytes);
        checkState(initialRevocableBytes == currentRevocableBytes || revocableMemoryAllocator.trySetBytes(currentRevocableBytes));
    }

    private static long getCacheEntrySize(SplitKey key, List<Page> pages)
    {
        long pagesRetainedSizeInBytes = 0L;
        for (Page page : pages) {
            pagesRetainedSizeInBytes += page.getRetainedSizeInBytes();
        }
        return getCacheEntrySize(key, pagesRetainedSizeInBytes);
    }

    private static long getCacheEntrySize(SplitKey key, long pagesRetainedSizeInBytes)
    {
        // account for splitCache entry memory
        return MAP_ENTRY_SIZE + key.getRetainedSizeInBytes() + pagesRetainedSizeInBytes +
                // account for splitLoaded entry
                MAP_ENTRY_SIZE + SETTABLE_FUTURE_INSTANCE_SIZE;
    }

    private class MemorySplitCache
            implements SplitCache
    {
        private final long signatureId;
        private volatile boolean closed;

        private MemorySplitCache(long signatureId)
        {
            this.signatureId = signatureId;
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId)
        {
            checkState(!closed, "MemorySplitCache already closed");
            return MemoryCacheManager.this.loadPages(signatureId, splitId);
        }

        @Override
        public Optional<ConnectorPageSink> storePages(CacheSplitId splitId)
        {
            checkState(!closed, "MemorySplitCache already closed");
            return MemoryCacheManager.this.storePages(signatureId, splitId);
        }

        @Override
        public void close()
        {
            checkState(!closed, "MemorySplitCache already closed");
            closed = true;
            releaseSignatureId(signatureId);
        }
    }

    private class MemoryCachePageSink
            implements ConnectorPageSink
    {
        private final SplitKey key;
        private final List<Page> pages = new ArrayList<>();
        private long memoryUsageBytes;

        public MemoryCachePageSink(SplitKey key)
        {
            this.key = requireNonNull(key, "key is null");
        }

        @Override
        public long getMemoryUsage()
        {
            return memoryUsageBytes;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            page.compact();
            pages.add(page);
            memoryUsageBytes += page.getRetainedSizeInBytes();
            return completedFuture(null);
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            finishStorePages(key, memoryUsageBytes, pages);
            return completedFuture(ImmutableList.of());
        }

        @Override
        public void abort()
        {
            abortStorePages(key);
        }
    }

    private static class MemoryPreferredAddressProvider
            implements PreferredAddressProvider
    {
        private final long signatureHash;
        private final Supplier<List<Node>> nodesSupplier;

        public MemoryPreferredAddressProvider(PlanSignature signature, NodeManager nodeManager)
        {
            signatureHash = signature.hashCode();
            nodesSupplier = Suppliers.memoizeWithExpiration(
                    () -> nodeManager.getWorkerNodes()
                            .stream()
                            .sorted(Comparator.comparing(Node::getHost))
                            .collect(toImmutableList()),
                    WORKER_NODES_CACHE_TIMEOUT_SECS,
                    SECONDS);
        }

        @Override
        public HostAddress getPreferredAddress(CacheSplitId splitId)
        {
            List<Node> nodes = nodesSupplier.get();
            return nodes.get(consistentHash(31 * signatureHash + splitId.hashCode(), nodes.size())).getHostAndPort();
        }
    }

    @VisibleForTesting
    record SplitKey(long signatureId, CacheSplitId splitId)
    {
        static final int INSTANCE_SIZE = instanceSize(SplitKey.class);

        public SplitKey(long signatureId, CacheSplitId splitId)
        {
            this.signatureId = signatureId;
            this.splitId = requireNonNull(splitId, "splitId is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SplitKey splitKey = (SplitKey) o;
            return signatureId == splitKey.signatureId && splitId.equals(splitKey.splitId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(signatureId, splitId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("signatureId", signatureId)
                    .add("splitId", splitId)
                    .toString();
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + splitId.getRetainedSizeInBytes();
        }
    }
}
