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
package io.trino.cache;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;

public class CacheConfig
{
    private boolean enabled;
    private double revokingThreshold = 0.9;
    private double revokingTarget = 0.7;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("cache.enabled")
    @ConfigDescription("Enables pipeline level cache")
    public CacheConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getRevokingThreshold()
    {
        return revokingThreshold;
    }

    @Config("cache.revoking-threshold")
    @ConfigDescription("Revoke cache memory when memory pool is filled over threshold")
    public CacheConfig setRevokingThreshold(double revokingThreshold)
    {
        this.revokingThreshold = revokingThreshold;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getRevokingTarget()
    {
        return revokingTarget;
    }

    @Config("cache.revoking-target")
    @ConfigDescription("When revoking cache memory, revoke so much that cache memory reservation is below target at the end")
    public CacheConfig setRevokingTarget(double revokingTarget)
    {
        this.revokingTarget = revokingTarget;
        return this;
    }
}
