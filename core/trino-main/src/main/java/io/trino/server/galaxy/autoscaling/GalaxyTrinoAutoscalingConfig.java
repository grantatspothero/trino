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

public class GalaxyTrinoAutoscalingConfig
{
    private Duration nodeStartupTime = Duration.succinctDuration(60, TimeUnit.SECONDS);
    private Duration remainingTimeScaleUpThreshold = Duration.succinctDuration(20, TimeUnit.SECONDS);
    private Duration remainingTimeScaleDownThreshold = Duration.succinctDuration(10, TimeUnit.SECONDS);

    private double scaleDownRatio = 0.8;
    private double scaleUpRatio = 1.5;

    public Duration getNodeStartupTime()
    {
        return nodeStartupTime;
    }

    @Config("galaxy.autoscaling.node-startup-time")
    @ConfigDescription("Time required to bring new worker online")
    public GalaxyTrinoAutoscalingConfig setNodeStartupTime(Duration nodeStartupTime)
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
    public GalaxyTrinoAutoscalingConfig setRemainingTimeScaleUpThreshold(Duration remainingTimeScaleUpThreshold)
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
    public GalaxyTrinoAutoscalingConfig setRemainingTimeScaleDownThreshold(Duration remainingTimeScaleDownThreshold)
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
    public GalaxyTrinoAutoscalingConfig setScaleDownRatio(double scaleDownRatio)
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
    public GalaxyTrinoAutoscalingConfig setScaleUpRatio(double scaleUpRatio)
    {
        this.scaleUpRatio = scaleUpRatio;
        return this;
    }
}
