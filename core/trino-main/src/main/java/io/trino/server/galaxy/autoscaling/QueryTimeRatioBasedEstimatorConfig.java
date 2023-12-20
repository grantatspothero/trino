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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class QueryTimeRatioBasedEstimatorConfig
{
    private Duration nodeStartupTime = new Duration(60, TimeUnit.SECONDS);
    private Duration remainingTimeScaleUpThreshold = new Duration(20, TimeUnit.SECONDS);
    private Duration remainingTimeScaleDownThreshold = new Duration(10, TimeUnit.SECONDS);
    private double scaleDownRatio = 0.8;
    private double scaleUpRatio = 1.5;

    public Duration getNodeStartupTime()
    {
        return nodeStartupTime;
    }

    @Config("galaxy.autoscaling.node-startup-time")
    @ConfigDescription("Time required to bring new worker online")
    public QueryTimeRatioBasedEstimatorConfig setNodeStartupTime(Duration nodeStartupTime)
    {
        this.nodeStartupTime = nodeStartupTime;
        return this;
    }

    public Duration getRemainingTimeScaleUpThreshold()
    {
        return remainingTimeScaleUpThreshold;
    }

    @Config("galaxy.autoscaling.remaining-time-scale-up-threshold")
    @ConfigDescription("Minimal remaining time to keep resized cluster busy - before considering scaleup")
    public QueryTimeRatioBasedEstimatorConfig setRemainingTimeScaleUpThreshold(Duration remainingTimeScaleUpThreshold)
    {
        this.remainingTimeScaleUpThreshold = remainingTimeScaleUpThreshold;
        return this;
    }

    public Duration getRemainingTimeScaleDownThreshold()
    {
        return remainingTimeScaleDownThreshold;
    }

    @Config("galaxy.autoscaling.remaining-time-scale-down-threshold")
    @ConfigDescription("The lower bound for a running time before considering scaledown.")
    public QueryTimeRatioBasedEstimatorConfig setRemainingTimeScaleDownThreshold(Duration remainingTimeScaleDownThreshold)
    {
        this.remainingTimeScaleDownThreshold = remainingTimeScaleDownThreshold;
        return this;
    }

    public double getScaleDownRatio()
    {
        return scaleDownRatio;
    }

    @Config("galaxy.autoscaling.scale-down-ratio")
    @ConfigDescription("Defines how big the single step of resize is when down scaling, new-nodes = current-nodes * scale-down-ratio")
    public QueryTimeRatioBasedEstimatorConfig setScaleDownRatio(double scaleDownRatio)
    {
        this.scaleDownRatio = scaleDownRatio;
        return this;
    }

    public double getScaleUpRatio()
    {
        return scaleUpRatio;
    }

    @Config("galaxy.autoscaling.scale-up-ratio")
    @ConfigDescription("Defines how big the single step of resize is when up scaling, new-nodes = current-nodes * scale-up-ratio")
    public QueryTimeRatioBasedEstimatorConfig setScaleUpRatio(double scaleUpRatio)
    {
        this.scaleUpRatio = scaleUpRatio;
        return this;
    }
}
