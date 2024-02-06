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
import com.google.common.base.Ticker;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class WorkerRecommendationProvider
{
    private static final Logger log = Logger.get(WorkerRecommendationProvider.class);

    private final InternalNodeManager nodeManager;
    private final boolean includeCoordinator;
    private final Duration refreshInterval;

    private final AtomicBoolean started = new AtomicBoolean();
    private final ScheduledExecutorService queryMetricsExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("autoscaling-query-metrics-%s"));

    private final QueryStatsCalculator statsCalculator;
    private final WorkerCountEstimator workerCountEstimator;

    private final TrinoAutoscalingStats trinoAutoscalerStats;

    @Inject
    public WorkerRecommendationProvider(
            NodeSchedulerConfig nodeSchedulerConfig,
            GalaxyTrinoAutoscalingConfig config,
            InternalNodeManager nodeManager,
            DispatchManager dispatchManager,
            WorkerCountEstimator workerCountEstimator,
            TrinoAutoscalingStats trinoAutoscalerStats)
    {
        this.includeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(dispatchManager, "dispatchManager is null");
        requireNonNull(config, "config is null");
        this.refreshInterval = Duration.succinctDuration(10, TimeUnit.SECONDS);
        this.statsCalculator = new QueryStatsCalculator(
                Ticker.systemTicker(),
                dispatchManager::getStats,
                dispatchManager::getQueries,
                this::getActiveNodes);
        this.workerCountEstimator = requireNonNull(workerCountEstimator, "workerCountEstimator is null");
        this.trinoAutoscalerStats = requireNonNull(trinoAutoscalerStats, "trinoAutoscalerStats is null");
    }

    @PostConstruct
    public void start()
    {
        if (!started.getAndSet(true)) {
            queryMetricsExecutor.scheduleAtFixedRate(
                    () -> {
                        try {
                            statsCalculator.calculateQueryStats();
                            trinoAutoscalerStats.getSuccessfulRuns().update(1);
                        }
                        catch (Throwable t) {
                            trinoAutoscalerStats.getFailedRuns().update(1);
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
        // Calculate inputs
        long cpuTimeToProcessQueriesMillis = estimateCpuTimeToProcessQueriesMillis(
                statsCalculator.getPastQueryStats(),
                statsCalculator.getRunningQueryStats(),
                statsCalculator.getQueuedQueries());
        double averageWorkerParallelism = statsCalculator.getAverageWorkerParallelism();
        int activeNodes = getActiveNodes();

        // Estimate sizing
        int recommendedNodes = workerCountEstimator.estimate(
                cpuTimeToProcessQueriesMillis,
                averageWorkerParallelism,
                activeNodes);

        trinoAutoscalerStats.updateEstimatorMetrics(
                cpuTimeToProcessQueriesMillis,
                averageWorkerParallelism,
                activeNodes,
                recommendedNodes);
        log.info("TrinoWorkers recommendation: %s; Inputs: (timeToProcess: %s, averageWorkerParallelism: %s, activeNodes: %s); Stats age: %s",
                recommendedNodes, cpuTimeToProcessQueriesMillis, averageWorkerParallelism, activeNodes, statsCalculator.getAge());

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

    public interface WorkerCountEstimator
    {
        int estimate(
                long cpuTimeToProcessQueriesMillis,
                double averageWorkerParallelism,
                int activeNodes);
    }
}
