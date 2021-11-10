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
package io.trino.server.galaxy.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CatalogMetricsSnapshot
{
    private final String catalogId;
    private final String catalogName;
    private final long intraRegionReadBytes;
    private final long intraRegionWriteBytes;
    private final long crossRegionReadBytes;
    private final long crossRegionWriteBytes;

    public CatalogMetricsSnapshot(String catalogId, String catalogName)
    {
        this(catalogId, catalogName, 0, 0, 0, 0);
    }

    public CatalogMetricsSnapshot(String catalogId, String catalogName, long intraRegionReadBytes, long intraRegionWriteBytes, long crossRegionReadBytes, long crossRegionWriteBytes)
    {
        this.catalogId = requireNonNull(catalogId, "catalogId is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        checkArgument(intraRegionReadBytes >= 0, "intraRegionReadBytes must be >= 0");
        this.intraRegionReadBytes = intraRegionReadBytes;
        checkArgument(intraRegionWriteBytes >= 0, "intraRegionWriteBytes must be >= 0");
        this.intraRegionWriteBytes = intraRegionWriteBytes;
        checkArgument(crossRegionReadBytes >= 0, "crossRegionReadBytes must be >= 0");
        this.crossRegionReadBytes = crossRegionReadBytes;
        checkArgument(crossRegionWriteBytes >= 0, "crossRegionWriteBytes must be >= 0");
        this.crossRegionWriteBytes = crossRegionWriteBytes;
    }

    @JsonProperty
    public String getCatalogId()
    {
        return catalogId;
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public long getIntraRegionReadBytes()
    {
        return intraRegionReadBytes;
    }

    @JsonProperty
    public long getIntraRegionWriteBytes()
    {
        return intraRegionWriteBytes;
    }

    @JsonProperty
    public long getCrossRegionReadBytes()
    {
        return crossRegionReadBytes;
    }

    @JsonProperty
    public long getCrossRegionWriteBytes()
    {
        return crossRegionWriteBytes;
    }

    public CatalogMetricsSnapshot minus(CatalogMetricsSnapshot other)
    {
        return new CatalogMetricsSnapshot(
                catalogId,
                catalogName,
                intraRegionReadBytes - other.intraRegionReadBytes,
                intraRegionWriteBytes - other.intraRegionWriteBytes,
                crossRegionReadBytes - other.crossRegionReadBytes,
                crossRegionWriteBytes - other.crossRegionWriteBytes);
    }
}
