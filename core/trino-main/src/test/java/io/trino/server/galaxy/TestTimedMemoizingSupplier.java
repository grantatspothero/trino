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
package io.trino.server.galaxy;

import io.airlift.testing.TestingClock;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.server.galaxy.TimedMemoizingSupplier.createStats;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.Thread.State.WAITING;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

public class TestTimedMemoizingSupplier
{
    @Test
    @Timeout(value = 10, unit = SECONDS, threadMode = SEPARATE_THREAD)
    public void testDelegate()
    {
        AtomicInteger loads = new AtomicInteger();
        TimedMemoizingSupplier<String> memoizingSupplier = new TimedMemoizingSupplier<>(
                noopTracer(),
                "some-name",
                () -> {
                    loads.incrementAndGet();
                    return "abc";
                },
                createStats());

        assertThat(memoizingSupplier.get(Instant.now())).isEqualTo("abc");
        assertThat(loads.get()).isEqualTo(1);
    }

    @Test
    @Timeout(value = 10, unit = SECONDS, threadMode = SEPARATE_THREAD)
    public void testNullValue()
    {
        AtomicInteger loads = new AtomicInteger();
        TimedMemoizingSupplier<String> memoizingSupplier = new TimedMemoizingSupplier<>(
                noopTracer(),
                "some-name",
                () -> {
                    loads.incrementAndGet();
                    return null;
                },
                createStats());

        assertThat(memoizingSupplier.get(Instant.now())).isNull();
        assertThat(loads.get()).isEqualTo(1);
    }

    @Test
    @Timeout(value = 10, unit = SECONDS, threadMode = SEPARATE_THREAD)
    public void testReuse()
    {
        TestingClock clock = new TestingClock();
        AtomicInteger loads = new AtomicInteger();
        TimedMemoizingSupplier<String> memoizingSupplier = new TimedMemoizingSupplier<>(
                clock,
                noopTracer(),
                "some-name",
                () -> "some value @ load #" + loads.incrementAndGet(),
                createStats());

        Instant firstProcessRequirements = clock.instant();
        clock.increment(10, MILLISECONDS);
        Instant secondProcessRequirements = clock.instant();
        clock.increment(10, MILLISECONDS);

        assertThat(memoizingSupplier.get(firstProcessRequirements)).isEqualTo("some value @ load #1");
        assertThat(loads.get()).isEqualTo(1);
        clock.increment(10, MILLISECONDS);

        Instant thirdProcessRequirements = clock.instant();
        clock.increment(10, MILLISECONDS);

        // The recently cached value was calculated after process #2 requirements, so it is reused
        assertThat(memoizingSupplier.get(secondProcessRequirements)).isEqualTo("some value @ load #1");
        assertThat(loads.get()).isEqualTo(1);
        clock.increment(10, MILLISECONDS);

        // The value was loaded before process #3 requirements, so the call gets through to the delegate
        assertThat(memoizingSupplier.get(thirdProcessRequirements)).isEqualTo("some value @ load #2");
        assertThat(loads.get()).isEqualTo(2);

        // New calls use the most fresh value
        assertThat(memoizingSupplier.get(firstProcessRequirements)).isEqualTo("some value @ load #2");
        assertThat(memoizingSupplier.get(secondProcessRequirements)).isEqualTo("some value @ load #2");
        assertThat(loads.get()).isEqualTo(2);
    }

    @Test
    @Timeout(value = 10, unit = SECONDS, threadMode = SEPARATE_THREAD)
    public void testSameInstant()
    {
        TestingClock clock = new TestingClock();
        AtomicInteger loads = new AtomicInteger();
        TimedMemoizingSupplier<String> memoizingSupplier = new TimedMemoizingSupplier<>(
                clock,
                noopTracer(),
                "some-name",
                () -> "some value @ load #" + loads.incrementAndGet(),
                createStats());

        Instant requirements = clock.instant();

        assertThat(memoizingSupplier.get(requirements)).isEqualTo("some value @ load #1");
        assertThat(loads.get()).isEqualTo(1);

        // If requirements=load time, the value is not reused. 1ms is eternity, so in requirements=load case the loaded value can actually be stale
        assertThat(memoizingSupplier.get(requirements)).isEqualTo("some value @ load #2");
        assertThat(loads.get()).isEqualTo(2);
    }

    @Test
    @Timeout(value = 10, unit = SECONDS, threadMode = SEPARATE_THREAD)
    public void testLoadSharing()
            throws Exception
    {
        try (ExecutorService executor = newCachedThreadPool()) {
            CountDownLatch loadStarted = new CountDownLatch(1);
            CountDownLatch allowLoadComplete = new CountDownLatch(1);
            try {
                TestingClock clock = new TestingClock();
                AtomicInteger loads = new AtomicInteger();
                TimedMemoizingSupplier<String> memoizingSupplier = new TimedMemoizingSupplier<>(
                        clock,
                        noopTracer(),
                        "some-name",
                        () -> {
                            int loadNumber = loads.incrementAndGet();
                            loadStarted.countDown();
                            await(allowLoadComplete);
                            return "some value @ load #" + loadNumber;
                        },
                        createStats());

                Instant requirements = clock.instant();
                clock.increment(1, MILLISECONDS);

                Future<String> firstCall = executor.submit(() -> memoizingSupplier.get(requirements));
                Future<String> secondCall = executor.submit(() -> memoizingSupplier.get(requirements));
                loadStarted.await();
                assertThat(loads.get()).isEqualTo(1);
                assertThat(firstCall).isNotDone();
                assertThat(secondCall).isNotDone();

                allowLoadComplete.countDown();
                assertThat(firstCall).succeedsWithin(1, SECONDS).isEqualTo("some value @ load #1");
                assertThat(secondCall).succeedsWithin(1, SECONDS).isEqualTo("some value @ load #1");
                assertThat(loads.get()).isEqualTo(1);
            }
            finally {
                allowLoadComplete.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = SECONDS, threadMode = SEPARATE_THREAD)
    public void testNoLoadSharingWhenRequirementsSameMilli()
            throws Exception
    {
        try (ExecutorService executor = newCachedThreadPool()) {
            CountDownLatch allowLoadComplete = new CountDownLatch(1);
            CountDownLatch firstLoadStarted = new CountDownLatch(1);
            CountDownLatch secondLoadStarted = new CountDownLatch(2);
            try {
                TestingClock clock = new TestingClock();
                AtomicInteger loads = new AtomicInteger();
                TimedMemoizingSupplier<String> memoizingSupplier = new TimedMemoizingSupplier<>(
                        clock,
                        noopTracer(),
                        "some-name",
                        () -> {
                            int loadNumber = loads.incrementAndGet();
                            firstLoadStarted.countDown();
                            secondLoadStarted.countDown();
                            await(allowLoadComplete);
                            return "some value @ load #" + loadNumber;
                        },
                        createStats());

                Instant requirements = clock.instant();
                Future<String> firstCall = executor.submit(() -> memoizingSupplier.get(requirements));
                firstLoadStarted.await();
                Future<String> secondCall = executor.submit(() -> memoizingSupplier.get(requirements));
                secondLoadStarted.await();
                assertThat(loads.get()).isEqualTo(2);
                assertThat(firstCall).isNotDone();
                assertThat(secondCall).isNotDone();

                allowLoadComplete.countDown();
                assertThat(loads.get()).isEqualTo(2);
                assertThat(firstCall).succeedsWithin(1, SECONDS).isEqualTo("some value @ load #1");
                assertThat(secondCall).succeedsWithin(1, SECONDS).isEqualTo("some value @ load #2");
            }
            finally {
                allowLoadComplete.countDown();
                firstLoadStarted.countDown();
                secondLoadStarted.countDown();
                secondLoadStarted.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = SECONDS, threadMode = SEPARATE_THREAD)
    public void testShareOldestSufficientLoad()
            throws Exception
    {
        try (ExecutorService executor = newCachedThreadPool()) {
            CyclicBarrier loadStarted = new CyclicBarrier(2);
            CountDownLatch allowLoadComplete = new CountDownLatch(1);
            try {
                TestingClock clock = new TestingClock();
                AtomicInteger loads = new AtomicInteger();
                TimedMemoizingSupplier<String> memoizingSupplier = new TimedMemoizingSupplier<>(
                        clock,
                        noopTracer(),
                        "some-name",
                        () -> {
                            int loadNumber = loads.incrementAndGet();
                            await(loadStarted);
                            await(allowLoadComplete);
                            return "some value @ load #" + loadNumber;
                        },
                        createStats());

                clock.increment(1, MILLISECONDS);
                Instant requirements1 = clock.instant();
                clock.increment(1, MILLISECONDS);
                Future<String> load1 = executor.submit(() -> memoizingSupplier.get(requirements1));
                loadStarted.await();

                clock.increment(1, MILLISECONDS);
                Instant requirements2 = clock.instant();
                clock.increment(1, MILLISECONDS);
                Instant requirements2Prime = clock.instant();
                clock.increment(1, MILLISECONDS);
                Future<String> load2 = executor.submit(() -> memoizingSupplier.get(requirements2));
                loadStarted.await();

                clock.increment(1, MILLISECONDS);
                Instant requirements3 = clock.instant();
                clock.increment(1, MILLISECONDS);
                Future<String> load3 = executor.submit(() -> memoizingSupplier.get(requirements3));
                loadStarted.await();

                clock.increment(1, MILLISECONDS);
                Instant requirements4 = clock.instant();
                clock.increment(1, MILLISECONDS);
                Future<String> load4 = executor.submit(() -> memoizingSupplier.get(requirements4));
                loadStarted.await();

                assertThat(loads.get()).isEqualTo(4);
                assertThat(load1).isNotDone();
                assertThat(load2).isNotDone();
                assertThat(load3).isNotDone();
                assertThat(load4).isNotDone();

                // This should re-use a future, not start a new load
                AtomicReference<Thread> submissionThread = new AtomicReference<>();
                Future<String> load2Prime = executor.submit(() -> {
                    submissionThread.set(Thread.currentThread());
                    return memoizingSupplier.get(requirements2Prime);
                });
                assertThat(loads.get()).isEqualTo(4);
                // To ensure load2Prime sees 4 futures still incomplete, release the allowLoadComplete latch only after the call blocks, presumably on the future.
                assertEventually(() -> assertThat(submissionThread.get().getState()).isEqualTo(WAITING));

                allowLoadComplete.countDown();
                assertThat(load1).succeedsWithin(1, SECONDS).isEqualTo("some value @ load #1");
                assertThat(load2).succeedsWithin(1, SECONDS).isEqualTo("some value @ load #2");
                assertThat(load3).succeedsWithin(1, SECONDS).isEqualTo("some value @ load #3");
                assertThat(load4).succeedsWithin(1, SECONDS).isEqualTo("some value @ load #4");

                // Currently the call which does not require new load reuses oldest sufficient future.
                // TODO it could use the first sufficient future to complete.
                assertThat(load2Prime).succeedsWithin(1, SECONDS).isEqualTo("some value @ load #2");
                assertThat(loads.get()).isEqualTo(4);
            }
            finally {
                allowLoadComplete.countDown();
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = SECONDS, threadMode = SEPARATE_THREAD)
    public void testStatsAndTracesWhenReuse()
    {
        try (InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
                SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                        .build()) {
            Tracer tracer = tracerProvider.get("test");
            TestingClock clock = new TestingClock();
            TimedMemoizingSupplier.Stats stats = createStats();
            TimedMemoizingSupplier<String> memoizingSupplier = new TimedMemoizingSupplier<>(
                    clock,
                    tracer,
                    "timed-sharing-supplier",
                    () -> "abc",
                    stats);

            Instant requirements1 = clock.instant();
            clock.increment(1, MILLISECONDS);
            memoizingSupplier.get(requirements1);
            assertThat(stats.getCalls().getTotalCount()).isEqualTo(1);
            assertThat(stats.getLoads().getTotalCount()).isEqualTo(1);
            assertThat(stats.getReuses().getTotalCount()).isEqualTo(0);
            assertThat(spansToString(spanExporter.getFinishedSpanItems()))
                    .containsExactlyInAnyOrder("timed-sharing-supplier {completion-mode=load}");

            memoizingSupplier.get(requirements1);
            assertThat(stats.getCalls().getTotalCount()).isEqualTo(2);
            assertThat(stats.getLoads().getTotalCount()).isEqualTo(1);
            assertThat(stats.getReuses().getTotalCount()).isEqualTo(1);
            assertThat(spansToString(spanExporter.getFinishedSpanItems()))
                    .containsExactlyInAnyOrder("timed-sharing-supplier {completion-mode=load}", "timed-sharing-supplier {completion-mode=ready}");
        }
    }

    @Test
    @Timeout(value = 10, unit = SECONDS, threadMode = SEPARATE_THREAD)
    public void testStatsAndTracesWhenLoadSharing()
            throws Exception
    {
        try (ExecutorService executor = newCachedThreadPool();
                InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
                SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                        .build()) {
            Tracer tracer = tracerProvider.get("test");
            CountDownLatch loadStarted = new CountDownLatch(1);
            CountDownLatch allowLoadComplete = new CountDownLatch(1);
            try {
                TestingClock clock = new TestingClock();
                TimedMemoizingSupplier.Stats stats = createStats();
                TimedMemoizingSupplier<String> memoizingSupplier = new TimedMemoizingSupplier<>(
                        clock,
                        tracer,
                        "concurrent-hot-sharing",
                        () -> {
                            loadStarted.countDown();
                            await(allowLoadComplete);
                            return "abc";
                        },
                        stats);

                Instant requirements = clock.instant();
                clock.increment(1, MILLISECONDS);

                Future<String> firstCall = executor.submit(() -> memoizingSupplier.get(requirements));
                loadStarted.await();
                AtomicReference<Thread> submissionThread = new AtomicReference<>();
                Future<String> secondCall = executor.submit(() -> {
                    submissionThread.set(Thread.currentThread());
                    return memoizingSupplier.get(requirements);
                });
                // To ensure the load is shared we need to ensure second call find the future, not the computed value
                // So release the allowLoadComplete latch only after the second call blocks, presumably on the future.
                assertEventually(() -> assertThat(submissionThread.get().getState()).isEqualTo(WAITING));
                allowLoadComplete.countDown();
                firstCall.get();
                secondCall.get();
                assertThat(stats.getCalls().getTotalCount()).isEqualTo(2);
                assertThat(stats.getLoads().getTotalCount()).isEqualTo(1);
                assertThat(stats.getReuses().getTotalCount()).isEqualTo(1);
                assertThat(spansToString(spanExporter.getFinishedSpanItems()))
                        .containsExactlyInAnyOrder("concurrent-hot-sharing {completion-mode=load}", "concurrent-hot-sharing {completion-mode=load-sharing}");
            }
            finally {
                allowLoadComplete.countDown();
                executor.shutdownNow();
            }
        }
    }

    private static void await(CyclicBarrier barrier)
    {
        try {
            barrier.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }

    private static void await(CountDownLatch latch)
    {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static Stream<String> spansToString(Collection<SpanData> spans)
    {
        return spans.stream()
                .map(span -> "%s %s".formatted(
                        span.getName(),
                        span.getAttributes().asMap().entrySet().stream()
                                .collect(toMap(
                                        entry -> entry.getKey().toString(),
                                        Map.Entry::getValue,
                                        (a, b) -> {
                                            throw new UnsupportedOperationException("Duplicate key with values %s, %s".formatted(a, b));
                                        },
                                        TreeMap::new))));
    }
}
