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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class DrainState
{
    public static final String IF_IDLE_FOR_HEADER = "X-If-Idle-For";
    public static final String CLUSTER_START_TIME_HEADER = "X-Cluster-Start-Time";

    public enum Type
    {
        UNDRAINED,
        DRAINING,
        DRAINED
    }

    private final Type type;
    private final Optional<Duration> maxDrainTime;

    @JsonCreator
    public DrainState(
            @JsonProperty("type") Type type,
            @JsonProperty("maxDrainTime") Optional<Duration> maxDrainTime)
    {
        this.type = requireNonNull(type, "type is null");
        this.maxDrainTime = requireNonNull(maxDrainTime, "maxDrainTime is null");
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<Duration> getMaxDrainTime()
    {
        return maxDrainTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DrainState that = (DrainState) o;
        return type == that.type && Objects.equals(maxDrainTime, that.maxDrainTime);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, maxDrainTime);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("maxDrainTime", maxDrainTime)
                .toString();
    }
}
