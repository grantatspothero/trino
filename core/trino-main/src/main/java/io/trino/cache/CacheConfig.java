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
import io.airlift.units.DataSize;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;

public class CacheConfig
{
    private boolean enabled;
    private double revokingThreshold = 0.9;
    private double revokingTarget = 0.7;
    private boolean cacheSubqueriesEnabled;
    private DataSize cacheSubqueriesSize = DataSize.of(256, DataSize.Unit.MEGABYTE);

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

    public boolean isCacheSubqueriesEnabled()
    {
        return cacheSubqueriesEnabled;
    }

    @Config("cache.subqueries.enabled")
    @ConfigDescription("Enables caching of common subqueries when running a single query")
    public CacheConfig setCacheSubqueriesEnabled(boolean cacheSubqueriesEnabled)
    {
        this.cacheSubqueriesEnabled = cacheSubqueriesEnabled;
        return this;
    }

    public DataSize getMaximalSplitCacheSubqueriesSize()
    {
        return cacheSubqueriesSize;
    }

    @Config("cache.subqueries.max-cached-split-size")
    public CacheConfig setMaximalSplitCacheSubqueriesSize(DataSize cacheSubqueriesSize)
    {
        this.cacheSubqueriesSize = cacheSubqueriesSize;
        return this;
    }
}
