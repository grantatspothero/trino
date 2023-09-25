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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.stats.CounterStat;
import jakarta.annotation.Nullable;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

/**
 * A memoizing {@link Supplier}-like wrapper which provides
 * strong visibility guarantees "as of time or later".
 */
@ThreadSafe
public final class TimedMemoizingSupplier<T>
{
    private static final Clock SYSTEM_CLOCK = Clock.tickMillis(UTC);

    private final Clock clock;
    private final Supplier<T> delegate;
    private final Stats stats;

    @GuardedBy("this")
    private TimedValue<T> newestCompleted;

    @GuardedBy("this")
    private final NavigableMap<Long, Future<T>> ongoingLoadsByTimestamp = new TreeMap<>();

    public TimedMemoizingSupplier(Supplier<T> delegate, Stats stats)
    {
        this(SYSTEM_CLOCK, delegate, stats);
    }

    @VisibleForTesting
    TimedMemoizingSupplier(Clock clock, Supplier<T> delegate, Stats stats)
    {
        this.clock = requireNonNull(clock, "clock is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    /**
     * Obtains and returns "sufficiently fresh" value from the delegate {@link Supplier}.
     * Sufficiently fresh means a value obtained after {@code visibilityRequirement} point in time.
     * Time calculation is assumed rounded/truncated to milliseconds, so the "sufficiently fresh" value
     * must have been loaded strictly after the visibility requirements.
     *
     * @param visibilityRequirement value freshness requirements
     */
    public @Nullable T get(Instant visibilityRequirement)
    {
        stats.calls.update(1);

        // Time is discrete so add 1 to provide visibility guarantees.
        long requiredTimestamp = visibilityRequirement.toEpochMilli() + 1;

        boolean foundReady = false;
        @Nullable T value = null;
        Future<T> concurrentLoad = null;
        long loadTimestamp = 0;
        CompletableFuture<T> future = null;
        try {
            synchronized (this) {
                // Use previous successfully computed value if it's fresh enough.
                if (newestCompleted != null && requiredTimestamp <= newestCompleted.loadStart) {
                    foundReady = true;
                    value = newestCompleted.value;
                }
                else {
                    // Use the oldest sufficient future since it's most likely to finish first.
                    concurrentLoad = Optional.ofNullable(ongoingLoadsByTimestamp.ceilingEntry(requiredTimestamp))
                            .map(Map.Entry::getValue)
                            .orElse(null);
                    if (concurrentLoad == null) {
                        loadTimestamp = clock.millis();
                        future = new CompletableFuture<>();
                        // There might be multiple loads started at loadTimestamp if their visibilityRequirement=loadTimestamp (loadTimestamp < requiredTimestamp)
                        ongoingLoadsByTimestamp.putIfAbsent(loadTimestamp, future);
                    }
                }
            }

            if (foundReady) {
                stats.reuses.update(1);
            }
            else if (concurrentLoad != null) {
                // Load sharing happens for concurrent loads only, so does not affect the caller's ability to retry.
                // Load sharing shares exceptions as well.
                stats.reuses.update(1);
                value = getFutureValue(concurrentLoad);
            }
            else {
                // This thread got the privilege to do the load
                stats.loads.update(1);
                future.completeAsync(delegate, directExecutor());
                value = getDone(future);
                synchronized (this) {
                    if (newestCompleted == null || newestCompleted.loadStart < loadTimestamp) {
                        newestCompleted = new TimedValue<>(loadTimestamp, value);
                    }
                    ongoingLoadsByTimestamp.remove(loadTimestamp, future);
                }
            }

            return value;
        }
        catch (Throwable e) {
            try {
                if (future != null) {
                    synchronized (this) {
                        // Ensure ongoingLoadsByTimestamp are bounded in size by number of threads concurrently executing this.get().
                        ongoingLoadsByTimestamp.remove(loadTimestamp, future);
                    }
                    // Ensure no-one waits for every on the future.
                    future.cancel(false);
                }
            }
            catch (Throwable cleanupException) {
                if (cleanupException != e) {
                    e.addSuppressed(cleanupException);
                }
            }
            throw e;
        }
    }

    @Override
    public String toString()
    {
        synchronized (this) {
            return toStringHelper(getClass())
                    .add("delegate", delegate)
                    .add("newestCompleted", newestCompleted)
                    .add("ongoingLoadsByTimestamp", ongoingLoadsByTimestamp)
                    .toString();
        }
    }

    public static Stats createStats()
    {
        return new Stats();
    }

    private record TimedValue<T>(long loadStart, @Nullable T value)
    {
    }

    public static final class Stats
    {
        private final CounterStat calls = new CounterStat();
        private final CounterStat loads = new CounterStat();
        private final CounterStat reuses = new CounterStat();

        private Stats() {}

        @Managed
        @Nested
        public CounterStat getCalls()
        {
            return calls;
        }

        @Managed
        @Nested
        public CounterStat getLoads()
        {
            return loads;
        }

        @Managed
        @Nested
        public CounterStat getReuses()
        {
            return reuses;
        }
    }
}
