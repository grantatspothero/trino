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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.DecayCounter;
import io.airlift.stats.ExponentialDecay;
import io.airlift.units.Duration;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryManagerStats;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.QueryId;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.common.math.DoubleMath.roundToLong;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class WorkerRecommendationProvider
{
    private static final Logger log = Logger.get(WorkerRecommendationProvider.class);

    private final InternalNodeManager nodeManager;
    private final DispatchManager dispatchManager;
    private final boolean includeCoordinator;
    private final Duration refreshInterval;

    private final AtomicBoolean started = new AtomicBoolean();
    private final ScheduledExecutorService queryMetricsExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("autoscaling-query-metrics-%s"));

    private final QueryStatsCalculator statsCalculator;
    private final WorkerCountEstimator workerCountEstimator;

    @Inject
    public WorkerRecommendationProvider(
            NodeSchedulerConfig nodeSchedulerConfig,
            GalaxyTrinoAutoscalingConfig config,
            InternalNodeManager nodeManager,
            DispatchManager dispatchManager,
            WorkerCountEstimator workerCountEstimator)
    {
        this.includeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        requireNonNull(config, "config is null");
        this.refreshInterval = Duration.succinctDuration(10, TimeUnit.SECONDS);
        this.statsCalculator = new QueryStatsCalculator(Ticker.systemTicker(), dispatchManager::getQueries, this::getActiveNodes);
        this.workerCountEstimator = requireNonNull(workerCountEstimator, "workerCountEstimator is null");
    }

    @PostConstruct
    public void start()
    {
        if (!started.getAndSet(true)) {
            queryMetricsExecutor.scheduleAtFixedRate(
                    () -> {
                        try {
                            statsCalculator.calculateQueryStats();
                        }
                        catch (Throwable t) {
                            log.error(t, "Error calculating running query metrics");
                        }
                    },
                    refreshInterval.toMillis(),
                    refreshInterval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    @PreDestroy
    public void destroy()
    {
        queryMetricsExecutor.shutdownNow();
    }

    public WorkerRecommendation get()
    {
        return calculateBasedOnQueryRunningTime();
    }

    private WorkerRecommendation calculateBasedOnQueryRunningTime()
    {
        // Given some stats
        QueryManagerStats stats = dispatchManager.getStats();
        QueryStats pastQueryStats = new QueryStats(
                (long) stats.getCompletedQueries().getFiveMinute().getCount(),
                stats.getConsumedCpuTimeSecs().getFiveMinute().getCount() * TimeUnit.SECONDS.toMillis(1));

        // Calculate inputs
        long cpuTimeToProcessQueriesMillis = estimateCpuTimeToProcessQueriesMillis(pastQueryStats, statsCalculator.getRunningQueryStats(), dispatchManager.getQueuedQueries());
        double avgWorkerParallelism = statsCalculator.avgWorkerParallelism();
        int activeNodes = getActiveNodes();

        // Estimate sizing
        int recommendedNodes = workerCountEstimator.estimate(
                cpuTimeToProcessQueriesMillis,
                avgWorkerParallelism,
                activeNodes);

        log.info("TrinoWorkers recommendation: %s; Inputs: (timeToProcess: %s, avgWorkerParallelism: %s, activeNodes: %s); Stats age: %s",
                recommendedNodes, cpuTimeToProcessQueriesMillis, avgWorkerParallelism, activeNodes, statsCalculator.getAge());

        return new WorkerRecommendation(recommendedNodes, Collections.emptyMap());
    }

    /*
     * Estimate the expected cpu time needed to finish current work
     */
    @VisibleForTesting
    static long estimateCpuTimeToProcessQueriesMillis(QueryStats pastQueryStats, QueryStats runningQueryStats, long queuedQueriesCount)
    {
        double pastQueryAverageCpuTime = pastQueryStats.queryCount() > 0 ?
                pastQueryStats.getAverageCpuTimePerQueryMillis() :
                runningQueryStats.getAverageCpuTimePerQueryMillis();
        double expectedCpuTimeToProcessQueue = queuedQueriesCount * pastQueryAverageCpuTime;

        double expectedCpuTimeToProcessRunning = runningQueryStats.queryCount() *
                approximateRemainingCpuTimeToProcessRunningQueries(runningQueryStats, pastQueryAverageCpuTime);

        return (long) (expectedCpuTimeToProcessQueue + expectedCpuTimeToProcessRunning);
    }

    private static double approximateRemainingCpuTimeToProcessRunningQueries(QueryStats runningQueryStats, double pastQueryAverageCpuTime)
    {
        return Math.max(runningQueryStats.getAverageCpuTimePerQueryMillis(), pastQueryAverageCpuTime - runningQueryStats.getAverageCpuTimePerQueryMillis());
    }

    private int getActiveNodes()
    {
        return (int) nodeManager.getNodes(NodeState.ACTIVE).stream()
                .filter(node -> includeCoordinator || !node.isCoordinator())
                .count();
    }

    public record QueryStats(long queryCount, double totalCpuTimeMillis)
    {
        public double getAverageCpuTimePerQueryMillis()
        {
            return queryCount == 0 ?
                    0 :
                    totalCpuTimeMillis / queryCount;
        }
    }

    private static class CpuMillisPerWorkerCounter
    {
        private Map<QueryId, Long> prevQueryIdToInfo = Collections.emptyMap();
        private final DecayCounter totalPerWorkerCpuMillis;

        public CpuMillisPerWorkerCounter(double alpha, Ticker ticker)
        {
            this.totalPerWorkerCpuMillis = new DecayCounter(alpha, requireNonNull(ticker, "ticker is null"));
        }

        public void update(Map<QueryId, Long> queryIdToQueryCpuTimeMillis, long workers)
        {
            queryIdToQueryCpuTimeMillis.forEach((id, totalQueryCpuMillis) -> {
                long deltaQueryCpuTimePerWorker = getDeltaCpuTime(id, totalQueryCpuMillis) / workers;
                totalPerWorkerCpuMillis.add(deltaQueryCpuTimePerWorker);
            });
            prevQueryIdToInfo = queryIdToQueryCpuTimeMillis;
        }

        private long getDeltaCpuTime(QueryId queryId, long totalQueryCpuTimeMillis)
        {
            long prevCpuTimeMillis = prevQueryIdToInfo.getOrDefault(queryId, 0L);
            return totalQueryCpuTimeMillis - prevCpuTimeMillis;
        }

        public double getCount()
        {
            return totalPerWorkerCpuMillis.getCount();
        }
    }

    private static class QueryStatsCalculator
    {
        final Ticker ticker;
        final Supplier<List<BasicQueryInfo>> queriesSupplier;
        final Supplier<Integer> nodeCountSupplier;

        // running queries
        private volatile QueryStats runningQueryStats = new QueryStats(0, 0);
        // Values to calculate parallelism / cluster computing power
        private final CpuMillisPerWorkerCounter totalPerWorkerCpuMillis;
        private final DecayCounter totalWallMillis;
        private final Stopwatch lastUpdateTimeStopwatch;

        public QueryStatsCalculator(Ticker ticker, Supplier<List<BasicQueryInfo>> queriesSupplier, Supplier<Integer> nodeCountSupplier)
        {
            this.ticker = requireNonNull(ticker, "ticker is null");
            this.queriesSupplier = requireNonNull(queriesSupplier, "queriesSupplier is null");
            this.nodeCountSupplier = requireNonNull(nodeCountSupplier, "nodeCountSupplier is null");
            this.totalPerWorkerCpuMillis = new CpuMillisPerWorkerCounter(ExponentialDecay.fiveMinutes(), ticker);
            this.totalWallMillis = new DecayCounter(ExponentialDecay.fiveMinutes(), ticker);
            this.lastUpdateTimeStopwatch = Stopwatch.createStarted(ticker);
        }

        public QueryStats getRunningQueryStats()
        {
            return runningQueryStats;
        }

        private synchronized void calculateQueryStats()
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
        }

        public double avgWorkerParallelism()
        {
            return totalPerWorkerCpuMillis.getCount() / totalWallMillis.getCount();
        }

        public Duration getAge()
        {
            return Duration.succinctDuration(lastUpdateTimeStopwatch.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        }
    }

    public interface WorkerCountEstimator
    {
        int estimate(
                long cpuTimeToProcessQueriesMillis,
                double avgWorkerParallelism,
                int activeNodes);
    }
}
