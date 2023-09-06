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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DeltaLakeInputInfo
{
    private final boolean partitioned;
    private final Map<String, String> galaxyTraits;

    @JsonCreator
    public DeltaLakeInputInfo(
            @JsonProperty("partitioned") boolean partitioned,
            @JsonProperty("galaxyTraits") Map<String, String> galaxyTraits)
    {
        this.partitioned = partitioned;
        this.galaxyTraits = requireNonNull(ImmutableMap.copyOf(galaxyTraits), "galaxyTraits are null");
    }

    @JsonProperty
    public boolean isPartitioned()
    {
        return partitioned;
    }

    @JsonProperty
    public Map<String, String> getGalaxyTraits()
    {
        return galaxyTraits;
    }

    @JsonProperty
    public String getTableType()
    {
        return "DELTA";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeltaLakeInputInfo that)) {
            return false;
        }
        return partitioned == that.partitioned
                && galaxyTraits.equals(that.galaxyTraits);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioned, galaxyTraits);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitioned", this.partitioned)
                .add("galaxyTraits", this.galaxyTraits)
                .toString();
    }
}
