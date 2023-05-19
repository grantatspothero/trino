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
import io.trino.spi.cache.MemoryAllocator;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SplitId;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.FixedPageSource;

import javax.annotation.concurrent.GuardedBy;

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
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.consistentHash;
import static java.lang.Math.max;
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
    private static final long WORKER_NODES_CACHE_TIMEOUT_SECS = 10;

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
        requireNonNull(context, "context is null");
        revocableMemoryAllocator = context.revocableMemoryAllocator();
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
    public synchronized void revokeMemory(long targetBytes)
    {
        checkArgument(targetBytes >= 0);
        long bytesToFree = max(allocatedRevocableBytes - targetBytes, 0);
        for (Iterator<Map.Entry<SplitKey, List<Page>>> iterator = splitCache.entrySet().iterator(); iterator.hasNext(); ) {
            if (bytesToFree <= 0) {
                break;
            }

            Map.Entry<SplitKey, List<Page>> entry = iterator.next();
            SplitKey key = entry.getKey();

            long splitRetainedSizeInBytes = 0;
            for (Page block : entry.getValue()) {
                splitRetainedSizeInBytes += block.getRetainedSizeInBytes();
            }

            iterator.remove();
            splitLoaded.remove(key);
            releaseSignatureId(key.signatureId());

            bytesToFree -= splitRetainedSizeInBytes;
            allocatedRevocableBytes -= splitRetainedSizeInBytes;
        }

        checkState(allocatedRevocableBytes >= 0);
        checkState(revocableMemoryAllocator.trySetBytes(allocatedRevocableBytes));
    }

    private synchronized Optional<ConnectorPageSource> loadPages(long signatureId, SplitId splitId)
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

    private synchronized Optional<ConnectorPageSink> storePages(long signatureId, SplitId splitId)
    {
        SplitKey key = new SplitKey(signatureId, splitId);
        SettableFuture<?> loaded = splitLoaded.get(key);
        if (loaded != null) {
            // split is already cached or currently being stored
            return Optional.empty();
        }

        acquireSignatureId(signatureId);
        splitLoaded.put(key, SettableFuture.create());
        return Optional.of(new MemoryCachePageSink(key));
    }

    private synchronized void finishStorePages(SplitKey key, long memoryUsageBytes, List<Page> pages)
    {
        if (!revocableMemoryAllocator.trySetBytes(allocatedRevocableBytes + memoryUsageBytes)) {
            // not sufficient memory to store split pages
            abortStorePages(key);
            return;
        }

        allocatedRevocableBytes += memoryUsageBytes;
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

    private class MemorySplitCache
            implements SplitCache
    {
        private final long signatureId;

        private MemorySplitCache(PlanSignature signature)
        {
            signatureId = allocateSignatureId(signature);
        }

        @Override
        public Optional<ConnectorPageSource> loadPages(SplitId splitId)
        {
            return MemoryCacheManager.this.loadPages(signatureId, splitId);
        }

        @Override
        public Optional<ConnectorPageSink> storePages(SplitId splitId)
        {
            return MemoryCacheManager.this.storePages(signatureId, splitId);
        }

        @Override
        public void close()
        {
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
        public HostAddress getPreferredAddress(SplitId splitId)
        {
            List<Node> nodes = nodesSupplier.get();
            return nodes.get(consistentHash(31 * signatureHash + splitId.hashCode(), nodes.size())).getHostAndPort();
        }
    }

    private record SplitKey(long signatureId, SplitId splitId)
    {
        public SplitKey(long signatureId, SplitId splitId)
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
    }
}
