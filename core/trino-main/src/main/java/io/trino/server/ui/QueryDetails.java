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
package io.trino.server.ui;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.TrinoWarning;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record QueryDetails(
        @JsonProperty("metadata") QueryMetadata metadata,
        @JsonProperty("statistics") QueryStatistics statistics,
        @JsonProperty("context") QueryContext context,
        @JsonProperty("ioMetadata") QueryIOMetadata ioMetadata,
        @JsonProperty("failureInfo") Optional<QueryFailureInfo> failureInfo,
        @JsonProperty("warnings") List<TrinoWarning> warnings,
        @JsonProperty("createTime") Instant createTime,
        @JsonProperty("executionStartTime") Optional<Instant> executionStartTime,
        @JsonProperty("endTime") Optional<Instant> endTime)
{
    public QueryDetails
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(statistics, "statistics is null");
        requireNonNull(context, "context is null");
        requireNonNull(ioMetadata, "ioMetadata is null");
        requireNonNull(failureInfo, "failureInfo is null");
        requireNonNull(warnings, "warnings is null");
        requireNonNull(createTime, "createTime is null");
        requireNonNull(executionStartTime, "executionStartTime is null");
        requireNonNull(endTime, "endTime is null");
    }
}
