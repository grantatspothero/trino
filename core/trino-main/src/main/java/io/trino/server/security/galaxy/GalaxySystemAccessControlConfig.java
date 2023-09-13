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

    @Min(1)
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
}
