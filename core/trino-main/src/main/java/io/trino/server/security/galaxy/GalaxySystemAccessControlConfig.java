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
package io.trino.server.security.galaxy;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Config for {@link GalaxyAccessControl} subsystem.
 * {@link GalaxyAccessControlConfig} but bound in the main context only (not twice).
 */
public class GalaxySystemAccessControlConfig
{
    public enum FilterColumnsAcceleration
    {
        NONE,
        FCX2,
    }

    private FilterColumnsAcceleration filterColumnsAcceleration = FilterColumnsAcceleration.NONE;
    private int backgroundProcessingThreads = 8;
    // Currently, we allow at most 60 concurrent queries (20 queries and 40 "data definition"), this value is with some margin.
    private int expectedQueryParallelism = 100;

    @NotNull
    public FilterColumnsAcceleration getFilterColumnsAcceleration()
    {
        return filterColumnsAcceleration;
    }

    @Config("galaxy.filter-columns-acceleration")
    public GalaxySystemAccessControlConfig setFilterColumnsAcceleration(FilterColumnsAcceleration filterColumnsAcceleration)
    {
        this.filterColumnsAcceleration = filterColumnsAcceleration;
        return this;
    }

    @Min(0)
    public int getBackgroundProcessingThreads()
    {
        return backgroundProcessingThreads;
    }

    @Config("galaxy.access-control-background-threads")
    public GalaxySystemAccessControlConfig setBackgroundProcessingThreads(int backgroundProcessingThreads)
    {
        this.backgroundProcessingThreads = backgroundProcessingThreads;
        return this;
    }

    @Min(1)
    public int getExpectedQueryParallelism()
    {
        return expectedQueryParallelism;
    }

    @Config("galaxy.expected-query-parallelism")
    @ConfigDescription("Expected query parallelism, should be the sum of hardConcurrencyLimit of all resource groups")
    public GalaxySystemAccessControlConfig setExpectedQueryParallelism(int expectedQueryParallelism)
    {
        this.expectedQueryParallelism = expectedQueryParallelism;
        return this;
    }
}
