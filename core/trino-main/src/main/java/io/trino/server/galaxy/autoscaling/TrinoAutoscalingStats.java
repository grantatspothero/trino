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

import com.google.common.util.concurrent.AtomicDouble;
import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.atomic.AtomicLong;

public class TrinoAutoscalingStats
{
    private final CounterStat successfulRuns = new CounterStat();
    private final CounterStat failedRuns = new CounterStat();
    private volatile long lastSuccessfulRunTimeMillis = System.currentTimeMillis();

    // estimator inputs
    private final AtomicLong cpuTimeToProcessQueriesMillis = new AtomicLong();
    private final AtomicDouble averageWorkerParallelism = new AtomicDouble();
    private final AtomicLong activeNodes = new AtomicLong();

    // estimator output
    private final AtomicLong recommendedReplicas = new AtomicLong();

    @Managed
    @Nested
    public CounterStat getSuccessfulRuns()
    {
        return successfulRuns;
    }

    @Managed
    @Nested
    public CounterStat getFailedRuns()
    {
        return failedRuns;
    }

    @Managed
    public long getRecommendedReplicas()
    {
        return recommendedReplicas.get();
    }

    public void updateRecommendedReplicas(long recommendedReplicas)
    {
        this.recommendedReplicas.set(recommendedReplicas);
    }

    @Managed
    public long getCpuTimeToProcessQueriesMillis()
    {
        return cpuTimeToProcessQueriesMillis.get();
    }

    public void updateCpuTimeToProcessQueriesMillis(long cpuTimeToProcessQueriesMillis)
    {
        this.cpuTimeToProcessQueriesMillis.set(cpuTimeToProcessQueriesMillis);
    }

    @Managed
    public double getAverageWorkerParallelism()
    {
        return averageWorkerParallelism.get();
    }

    public void updateAverageWorkerParallelism(double averageWorkerParallelism)
    {
        this.averageWorkerParallelism.set(averageWorkerParallelism);
    }

    @Managed
    public long getActiveNodes()
    {
        return activeNodes.get();
    }

    public void updateActiveNodes(long activeNodes)
    {
        this.activeNodes.set(activeNodes);
    }

    @Managed
    public long getTimeSinceLastSuccessMillis()
    {
        return System.currentTimeMillis() - lastSuccessfulRunTimeMillis;
    }

    public void setLastSuccessfulRunTimeMillis(long lastSuccessfulRunTimeMillis)
    {
        this.lastSuccessfulRunTimeMillis = lastSuccessfulRunTimeMillis;
    }

    public void updateEstimatorMetrics(long cpuTimeToProcessQueriesMillis, double averageWorkerParallelism, int activeNodes, int recommendedNodes)
    {
        updateCpuTimeToProcessQueriesMillis(cpuTimeToProcessQueriesMillis);
        updateAverageWorkerParallelism(averageWorkerParallelism);
        updateActiveNodes(activeNodes);
        updateRecommendedReplicas(recommendedNodes);
        setLastSuccessfulRunTimeMillis(System.currentTimeMillis());
    }
}
