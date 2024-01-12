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
package io.trino.server.galaxy.autoscaling;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import io.airlift.stats.DecayCounter;
import io.airlift.stats.ExponentialDecay;
import io.airlift.units.Duration;
import io.trino.execution.QueryManagerStats;
import io.trino.execution.QueryState;
import io.trino.server.BasicQueryInfo;
import io.trino.server.galaxy.autoscaling.WorkerRecommendationProvider.QueryStats;
import io.trino.spi.QueryId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.math.DoubleMath.roundToLong;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.WAITING_FOR_RESOURCES;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Objects.requireNonNull;

public class QueryStatsCalculator
{
    private final Supplier<QueryManagerStats> queryManagerStatsSupplier;
    private final Supplier<List<BasicQueryInfo>> queriesSupplier;
    private final Supplier<Integer> nodeCountSupplier;

    // running queries
    private volatile QueryStats pastQueryStats = new QueryStats(0, 0);
    private volatile QueryStats runningQueryStats = new QueryStats(0, 0);
    private volatile long queuedQueries;
    // Values to calculate parallelism / cluster computing power
    private final CpuMillisPerWorkerCounter totalPerWorkerCpuMillis;
    private final DecayCounter totalWallMillis;
    private final Stopwatch lastUpdateTimeStopwatch;

    public QueryStatsCalculator(Ticker ticker,
            Supplier<QueryManagerStats> queryManagerStatsSupplier,
            Supplier<List<BasicQueryInfo>> queriesSupplier,
            Supplier<Integer> nodeCountSupplier)
    {
        requireNonNull(ticker, "ticker is null");
        this.queryManagerStatsSupplier = requireNonNull(queryManagerStatsSupplier, "queryManagerStatsSupplier is null");
        this.queriesSupplier = requireNonNull(queriesSupplier, "queriesSupplier is null");
        this.nodeCountSupplier = requireNonNull(nodeCountSupplier, "nodeCountSupplier is null");
        this.totalPerWorkerCpuMillis = new CpuMillisPerWorkerCounter(ExponentialDecay.fiveMinutes(), ticker);
        this.totalWallMillis = new DecayCounter(ExponentialDecay.fiveMinutes(), ticker);
        this.lastUpdateTimeStopwatch = Stopwatch.createStarted(ticker);
    }

    public synchronized void calculateQueryStats()
    {
        long workers = nodeCountSupplier.get();

        long runningQueries = 0;
        long totalCpuTimeMillis = 0;

        Map<QueryId, Long> queryIdToCpuTime = new HashMap<>();
        for (BasicQueryInfo query : queriesSupplier.get()) {
            if (!query.getState().isDone()) {
                runningQueries++;

                long totalQueryCpuTimeMillis = roundToLong(query.getQueryStats().getTotalCpuTime().getValue(TimeUnit.MILLISECONDS), HALF_UP);
                totalCpuTimeMillis += totalQueryCpuTimeMillis;
                queryIdToCpuTime.put(query.getQueryId(), totalQueryCpuTimeMillis);
            }
        }
        if (runningQueries != 0) {
            totalPerWorkerCpuMillis.update(queryIdToCpuTime, workers);
            // total Wall Millis counts the time difference between current and previous sample
            totalWallMillis.add(lastUpdateTimeStopwatch.elapsed(TimeUnit.MILLISECONDS));
        }

        runningQueryStats = new QueryStats(runningQueries, totalCpuTimeMillis);
        lastUpdateTimeStopwatch.reset().start();

        QueryManagerStats stats = queryManagerStatsSupplier.get();
        pastQueryStats = new QueryStats(
                (long) stats.getCompletedQueries().getFiveMinute().getCount(),
                stats.getConsumedCpuTimeSecs().getFiveMinute().getCount() * TimeUnit.SECONDS.toMillis(1));
        queuedQueries = queriesSupplier.get().stream()
                .filter(query -> query.getState() == QUEUED)
                .count();
    }

    public QueryStats getPastQueryStats()
    {
        return pastQueryStats;
    }

    public QueryStats getRunningQueryStats()
    {
        return runningQueryStats;
    }

    public double getAverageWorkerParallelism()
    {
        return totalPerWorkerCpuMillis.getCount() / totalWallMillis.getCount();
    }

    public long getQueuedQueries()
    {
        return queuedQueries;
    }

    public Duration getAge()
    {
        return Duration.succinctDuration(lastUpdateTimeStopwatch.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }
}
