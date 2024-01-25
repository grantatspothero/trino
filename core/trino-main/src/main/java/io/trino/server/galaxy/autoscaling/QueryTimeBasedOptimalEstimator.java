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

import static com.google.common.math.DoubleMath.roundToInt;
import static java.math.RoundingMode.CEILING;
import static java.math.RoundingMode.FLOOR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class QueryTimeBasedOptimalEstimator
        implements WorkerRecommendationProvider.WorkerCountEstimator
{
    private final long nodeStartupTime;

    /*
     * ScaleUpThresholdSeconds is a minimal time we want to run a cluster after scaling up.
     * If runtime is shorter than threshold it is not worth to scale up.
     * If there is not enough work to run cluster for at least the specified interval,
     * then it is not cost-efficient to spin up additional workers.
     */
    private final long scaleUpThresholdSeconds;
    /*
     * ScaleDownThresholdSecs is the lower bound for a running time before scaledown.
     * If there is not enough work to run for scaleDownThreshold Seconds, it means we can
     * resize the cluster.
     */
    private final long scaleDownThresholdSecs;
    /*
     * The TargetLatency specifies the maximum time to wait before next query can be started.
     * It is an approximation based on assumption that current processing takes 100% of cluster
     * and has to finish before next query can be started.
     */
    private final long targetLatency;

    private final double scaleDownRatio;

    @Inject
    public QueryTimeBasedOptimalEstimator(QueryTimeBasedOptimalEstimatorConfig config)
    {
        requireNonNull(config, "config is null");
        this.nodeStartupTime = config.getNodeStartupTime().roundTo(SECONDS);
        this.targetLatency = config.getTargetLatency().roundTo(SECONDS);
        this.scaleUpThresholdSeconds = config.getRemainingTimeScaleUpThreshold().roundTo(SECONDS);
        this.scaleDownThresholdSecs = config.getRemainingTimeScaleDownThreshold().roundTo(SECONDS);
        this.scaleDownRatio = config.getScaleDownRatio();
    }

    @Override
    public int estimate(
            long cpuTimeToProcessQueriesMillis,
            double averageWorkerParallelism,
            int activeNodes)
    {
        double cpuTimeToProcessQueriesSeconds = MILLISECONDS.toSeconds(cpuTimeToProcessQueriesMillis);
        double expectedRunningTimeSeconds = cpuTimeToProcessQueriesSeconds / averageWorkerParallelism / activeNodes;

        if (expectedRunningTimeSeconds < scaleDownThresholdSecs) {
            return roundToInt(activeNodes * scaleDownRatio, FLOOR);
        }

        // compute the time it will take to finish the remainingWork after waiting for rescale,
        // it is also possible to set nodeStartupTime to `0` and tune the behavior with only scaleUpThresholdSeconds
        double timeToFinishWorkAfterRescaleSecs = Math.max(0, expectedRunningTimeSeconds - nodeStartupTime);

        // we want to have NewTimeToFinishWork to less or equal targetLatency
        // we want to compute the optimal scale up to achieve targetLatency (or maybe lets call it targetMaximumRunningTime)
        // assuming linear scaling, when the cluster size is rescaled by a factor of X, the time `NewTimeToFinishWork == timeToFinishWork / X`
        // given
        // - NewTimeToFinishWork == timeToFinishWork / X
        // - NewTimeToFinishWork <= targetLatency
        // we can approximate scale:
        // X >= timeToFinishWork / targetLatency;
        double scale = timeToFinishWorkAfterRescaleSecs / targetLatency;
        if (scale > 1.0 && (timeToFinishWorkAfterRescaleSecs / scale) > scaleUpThresholdSeconds) {
            return roundToInt(activeNodes * scale, CEILING);
        }

        return activeNodes;
    }
}
