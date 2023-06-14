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
import com.google.inject.Inject;
import io.airlift.slice.Slice;
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

import javax.annotation.concurrent.GuardedBy;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.consistentHash;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static sun.misc.Unsafe.ARRAY_OBJECT_INDEX_SCALE;

/**
 * {@link CacheManager} implementation that caches split pages in revocable memory and does not have
 * support for any adaption (column pruning, filtering).
 */
public class MemoryCacheManager
        implements CacheManager
{
    // based on SizeOf.estimatedSizeOf(java.util.Map<K,V>, java.util.function.ToLongFunction<K>, java.util.function.ToLongFunction<V>)
    static final int MAP_ENTRY_SIZE = ARRAY_OBJECT_INDEX_SCALE + instanceSize(AbstractMap.SimpleEntry.class);
    static final int SETTABLE_FUTURE_INSTANCE_SIZE = instanceSize(SettableFuture.class);

    private static final int DEFAULT_MAX_PLAN_SIGNATURES = 200;
    private static final long WORKER_NODES_CACHE_TIMEOUT_SECS = 10;

    private final int maxPlanSignatures;
    private final MemoryAllocator revocableMemoryAllocator;

    @GuardedBy("this")
    private final Map<SplitKey, List<Page>> splitCache = new LinkedHashMap<>();
    @GuardedBy("this")
    private final Map<SplitKey, SettableFuture<?>> splitLoaded = new HashMap<>();
    /**
     * Comparing of {@link PlanSignature} per split can be expensive (e.g. when
     * {@link PlanSignature#getPredicate() is large}. Therefore {@link PlanSignature}s
     * are mapped to {@link Long} signatureIds.
     */
    @GuardedBy("this")
    private final Map<PlanSignature, Long> signatureToId = new HashMap<>();
    @GuardedBy("this")
    private final Map<Long, PlanSignature> idToSignature = new HashMap<>();
    /**
     * Usage count per signatureId. When usage count for particular
     * signatureId drops to 0, then corresponding mapping from {@link PlanSignature}
     * to signatureId can be dropped.
     */
    @GuardedBy("this")
    private final Map<Long, Long> idUsageCount = new HashMap<>();
    private final AtomicLong nextSignatureId = new AtomicLong();
    private long allocatedRevocableBytes;

    @Inject
    public MemoryCacheManager(CacheManagerContext context)
    {
        this(context, DEFAULT_MAX_PLAN_SIGNATURES);
    }

    @VisibleForTesting
    MemoryCacheManager(CacheManagerContext context, int maxPlanSignatures)
    {
        requireNonNull(context, "context is null");
        this.maxPlanSignatures = maxPlanSignatures;
        this.revocableMemoryAllocator = context.revocableMemoryAllocator();
    }

    @Override
    public SplitCache getSplitCache(PlanSignature signature)
    {
        return new MemorySplitCache(signature);
    }

    @Override
    public PreferredAddressProvider getPreferredAddressProvider(PlanSignature signature, NodeManager nodeManager)
    {
        return new MemoryPreferredAddressProvider(signature, nodeManager);
    }

    @Override
    public synchronized void revokeMemory(long bytesToRevoke)
    {
        checkArgument(bytesToRevoke >= 0);
        long initialAllocatedBytes = allocatedRevocableBytes;
        removeEldestSplits(() -> initialAllocatedBytes - allocatedRevocableBytes >= bytesToRevoke);
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

        acquireSignatureId(signatureId);
        // memory for splitLoaded entry will be accounted in finishStorePages
        splitLoaded.put(key, SettableFuture.create());
        return Optional.of(new MemoryCachePageSink(key));
    }

    private synchronized void finishStorePages(SplitKey key, long memoryUsageBytes, List<Page> pages)
    {
        // account for splitCache entry memory
        long entrySize = MAP_ENTRY_SIZE + key.getRetainedSizeInBytes() + memoryUsageBytes;
        // account for splitLoaded entry
        entrySize += MAP_ENTRY_SIZE + key.getRetainedSizeInBytes() + SETTABLE_FUTURE_INSTANCE_SIZE;
        if (!revocableMemoryAllocator.trySetBytes(allocatedRevocableBytes + entrySize)) {
            // not sufficient memory to store split pages
            abortStorePages(key);
            return;
        }

        allocatedRevocableBytes += entrySize;
        splitCache.put(key, pages);
        splitLoaded.get(key).set(null);
        checkState(idUsageCount.get(key.signatureId()) > 0, "Signature id must not be released while split is cached");
    }

    private synchronized void abortStorePages(SplitKey key)
    {
        splitLoaded.remove(key).set(null);
        releaseSignatureId(key.signatureId());
    }

    private synchronized long allocateSignatureId(PlanSignature signature)
    {
        Long signatureId = signatureToId.get(signature);
        if (signatureId == null) {
            signatureId = nextSignatureId.incrementAndGet();
            signatureToId.put(signature, signatureId);
            idToSignature.put(signatureId, signature);
            idUsageCount.put(signatureId, 0L);
        }

        acquireSignatureId(signatureId);
        return signatureId;
    }

    private synchronized void acquireSignatureId(long signatureId)
    {
        idUsageCount.put(signatureId, idUsageCount.get(signatureId) + 1);
    }

    private synchronized void releaseSignatureId(long signatureId)
    {
        long usageCount = idUsageCount.get(signatureId) - 1;
        checkState(usageCount >= 0, "Usage count is negative");
        idUsageCount.put(signatureId, usageCount);
        if (usageCount == 0) {
            signatureToId.remove(idToSignature.get(signatureId));
            idToSignature.remove(signatureId);
            idUsageCount.remove(signatureId);
        }
    }

    private synchronized void removeEldestSplits(BooleanSupplier stopCondition)
    {
        if (splitCache.isEmpty()) {
            // no splits to remove
            return;
        }

        for (Iterator<Map.Entry<SplitKey, List<Page>>> iterator = splitCache.entrySet().iterator(); iterator.hasNext(); ) {
            if (stopCondition.getAsBoolean()) {
                break;
            }

            Map.Entry<SplitKey, List<Page>> entry = iterator.next();
            SplitKey key = entry.getKey();

            // account for splitCache entry memory
            long splitRetainedSizeInBytes = MAP_ENTRY_SIZE + key.getRetainedSizeInBytes();
            for (Page block : entry.getValue()) {
                splitRetainedSizeInBytes += block.getRetainedSizeInBytes();
            }
            // account for splitLoaded entry memory
            splitRetainedSizeInBytes += MAP_ENTRY_SIZE + key.getRetainedSizeInBytes() + SETTABLE_FUTURE_INSTANCE_SIZE;

            iterator.remove();
            splitLoaded.remove(key);
            releaseSignatureId(key.signatureId());

            allocatedRevocableBytes -= splitRetainedSizeInBytes;
        }

        checkState(allocatedRevocableBytes >= 0);
        checkState(revocableMemoryAllocator.trySetBytes(allocatedRevocableBytes));
    }

    private class MemorySplitCache
            implements SplitCache
    {
        private final long signatureId;
        private volatile boolean closed;

        private MemorySplitCache(PlanSignature signature)
        {
            signatureId = allocateSignatureId(signature);
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
            // remove oldest cached splits in order to free plan signature slots
            removeEldestSplits(() -> signatureToId.size() <= maxPlanSignatures);
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
