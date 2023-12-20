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

import com.google.inject.Inject;
import io.trino.server.galaxy.autoscaling.WorkerRecommendationProvider.WorkerCountEstimator;

import java.util.concurrent.TimeUnit;

import static com.google.common.math.DoubleMath.roundToInt;
import static java.math.RoundingMode.CEILING;
import static java.math.RoundingMode.FLOOR;
import static java.util.Objects.requireNonNull;

public class QueryTimeRatioBasedEstimator
        implements WorkerCountEstimator
{
    /*
     * ScaleupTime is a time required to bring new worker online
     */
    private final long scaleUpTimeSeconds;

    /*
     * MinimalClusterRuntime is a minimal time we want to run a cluster after a resize.
     * If there is not enough work to run cluster for at least the specified interval,
     * then it is not cost-efficient to spin up additional workers.
     */
    private final long minimalClusterRuntimeSecs;
    /*
     * ScaleDownThresholdSecs is the lower bound for a running time before scaledown.
     * If there is not enough work to run for scaleDownThreshold Seconds, it means we can
     * resize the cluster.
     */
    private final long scaleDownThresholdSecs;
    private final double scaleDownRatio;
    private final double scaleUpRatio;

    @Inject
    public QueryTimeRatioBasedEstimator(GalaxyTrinoAutoscalingConfig config)
    {
        requireNonNull(config, "config is null");
        this.scaleUpTimeSeconds = config.getNodeStartupTime().roundTo(TimeUnit.SECONDS);
        this.minimalClusterRuntimeSecs = config.getRemainingTimeScaleUpThreshold().roundTo(TimeUnit.SECONDS);
        this.scaleDownThresholdSecs = config.getRemainingTimeScaleDownThreshold().roundTo(TimeUnit.SECONDS);
        this.scaleDownRatio = config.getScaleDownRatio();
        this.scaleUpRatio = config.getScaleUpRatio();
    }

    @Override
    public int estimate(
            long cpuTimeToProcessQueriesMillis,
            double avgWorkerParallelism,
            int activeNodes)
    {
        double cpuTimeToProcessQueriesSeconds = TimeUnit.MILLISECONDS.toSeconds(cpuTimeToProcessQueriesMillis);
        double expectedRunningTimeSeconds = cpuTimeToProcessQueriesSeconds / avgWorkerParallelism / activeNodes;

        if (expectedRunningTimeSeconds < scaleDownThresholdSecs) {
            return roundToInt(activeNodes * scaleDownRatio, FLOOR);
        }
        // COMPUTE the time it will take to finish work after waiting for rescale,
        double timeToFinishWorkAfterRescaleSecs = expectedRunningTimeSeconds - scaleUpTimeSeconds;

        // finally calculate if R on a scaled cluster is higher than threshold, so it can benefit from scaleup
        if ((timeToFinishWorkAfterRescaleSecs / scaleUpRatio) > minimalClusterRuntimeSecs) {
            return roundToInt(activeNodes * scaleUpRatio, CEILING);
        }

        return activeNodes;
    }
}
