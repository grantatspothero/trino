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
package io.trino.plugin.base.galaxy;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import jakarta.validation.constraints.NotNull;

public class CrossRegionConfig
{
    private boolean allowCrossRegionAccess;
    private DataSize crossRegionReadLimit = DataSize.of(20, Unit.GIGABYTE);
    private DataSize crossRegionWriteLimit = DataSize.of(10, Unit.GIGABYTE);

    public boolean getAllowCrossRegionAccess()
    {
        return allowCrossRegionAccess;
    }

    @Config("galaxy.cross-region.allow-cross-region-access")
    public CrossRegionConfig setAllowCrossRegionAccess(boolean allowCrossRegionAccess)
    {
        this.allowCrossRegionAccess = allowCrossRegionAccess;
        return this;
    }

    @NotNull
    public DataSize getCrossRegionReadLimit()
    {
        return crossRegionReadLimit;
    }

    @Config("galaxy.cross-region.read-limit")
    @ConfigDescription("The maximum amount of data that can be read across regions per worker")
    public CrossRegionConfig setCrossRegionReadLimit(DataSize crossRegionReadLimit)
    {
        this.crossRegionReadLimit = crossRegionReadLimit;
        return this;
    }

    @NotNull
    public DataSize getCrossRegionWriteLimit()
    {
        return crossRegionWriteLimit;
    }

    @Config("galaxy.cross-region.write-limit")
    @ConfigDescription("The maximum amount of data that can be written across regions per worker")
    public CrossRegionConfig setCrossRegionWriteLimit(DataSize crossRegionWriteLimit)
    {
        this.crossRegionWriteLimit = crossRegionWriteLimit;
        return this;
    }
}
