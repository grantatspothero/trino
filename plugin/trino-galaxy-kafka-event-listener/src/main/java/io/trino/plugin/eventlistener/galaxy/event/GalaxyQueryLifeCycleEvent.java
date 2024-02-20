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
package io.trino.plugin.eventlistener.galaxy.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.ErrorCode;
import io.trino.spi.Unstable;

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GalaxyQueryLifeCycleEvent
{
    private final String accountId;
    private final String clusterId;
    private final String deploymentId;
    private final String queryId;
    private final String state;
    private final Instant eventTime;
    private final String queryText;
    private final Optional<String> principal;
    private final Optional<ErrorCode> errorCode;

    @JsonCreator
    @Unstable
    public GalaxyQueryLifeCycleEvent(
            String accountId,
            String clusterId,
            String deploymentId,
            String queryId,
            String state,
            Instant eventTime,
            String queryText,
            Optional<String> principal,
            Optional<ErrorCode> errorCode)
    {
        this.accountId = requireNonNull(accountId, "accountId is null");
        this.clusterId = requireNonNull(clusterId, "clusterId is null");
        this.deploymentId = requireNonNull(deploymentId, "deploymentId is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.state = requireNonNull(state, "state is null");
        this.eventTime = requireNonNull(eventTime, "eventTime is null");
        this.queryText = requireNonNull(queryText, "queryText is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
    }

    @JsonProperty
    public String getAccountId()
    {
        return accountId;
    }

    @JsonProperty
    public String getClusterId()
    {
        return clusterId;
    }

    @JsonProperty
    public String getDeploymentId()
    {
        return deploymentId;
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public Instant getEventTime()
    {
        return eventTime;
    }

    @JsonProperty
    public String getQueryText()
    {
        return queryText;
    }

    @JsonProperty
    public Optional<String> getPrincipal()
    {
        return principal;
    }

    @JsonProperty
    public Optional<ErrorCode> getErrorCode()
    {
        return errorCode;
    }
}
