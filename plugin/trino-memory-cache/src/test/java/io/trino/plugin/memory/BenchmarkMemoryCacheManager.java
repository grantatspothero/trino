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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 6, time = 2)
@Measurement(iterations = 6, time = 2)
@BenchmarkMode(Throughput)
public class BenchmarkMemoryCacheManager
{
    @State(Scope.Benchmark)
    public static class Context
    {
        @Param({"false", "true"})
        private boolean polluteCache;

        private final MemoryCacheManager cacheManager = new MemoryCacheManager(() -> bytes -> bytes <= 4_000_000_000L, true);
        private final CacheSplitId splitId = new CacheSplitId("split");
        private final List<CacheColumnId> columnIds = IntStream.range(0, 64)
                .mapToObj(i -> new CacheColumnId("column" + i))
                .collect(toImmutableList());
        private final PlanSignature signature = new PlanSignature(
                new SignatureKey("key"),
                Optional.empty(),
                columnIds,
                TupleDomain.all(),
                TupleDomain.all());
        private final Page page = new Page(nCopies(
                columnIds.size(),
                new IntArrayBlock(4, Optional.empty(), new int[] {0, 1, 2, 3}))
                .toArray(new Block[0]));

        @Setup
        public void setup()
                throws IOException
        {
            if (polluteCache) {
                for (int i = 0; i < 100; i++) {
                    storeCachedData();
                }
            }
            storeCachedData();
        }

        public Optional<ConnectorPageSource> loadCachedData()
                throws IOException
        {
            try (CacheManager.SplitCache splitCache = cacheManager.getSplitCache(signature)) {
                return splitCache.loadPages(splitId);
            }
        }

        public void storeCachedData()
                throws IOException
        {
            try (CacheManager.SplitCache splitCache = cacheManager.getSplitCache(signature)) {
                ConnectorPageSink sink = splitCache.storePages(splitId).orElseThrow();
                sink.appendPage(page);
                sink.finish();
            }
        }
    }

    @Threads(10)
    @Benchmark
    public Optional<ConnectorPageSource> benchmarkLoadPages(Context context)
            throws IOException
    {
        return context.loadCachedData();
    }

    @Threads(10)
    @Benchmark
    public void benchmarkStorePages(Context context)
            throws IOException
    {
        context.storeCachedData();
    }

    @Test
    public void testBenchmarkLoadPages()
            throws IOException
    {
        Context context = new Context();
        context.setup();
        assertThat(benchmarkLoadPages(context)).isPresent();
    }

    @Test
    public void testBenchmarkStorePages()
            throws IOException
    {
        Context context = new Context();
        context.setup();
        benchmarkStorePages(context);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkMemoryCacheManager.class).run();
    }
}
