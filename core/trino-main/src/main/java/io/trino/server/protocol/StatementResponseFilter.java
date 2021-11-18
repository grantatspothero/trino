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
package io.trino.server.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.client.QueryResults;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.net.URLDecoder.decode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.OK;

@Provider
public class StatementResponseFilter
        implements ContainerResponseFilter
{
    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response)
    {
        if (request.getHeaderString("X-Trino-Prepared-Statement-In-Body") == null ||
                response.getStatus() != OK.getStatusCode() ||
                !response.getMediaType().equals(MediaType.APPLICATION_JSON_TYPE) ||
                !(response.getEntity() instanceof QueryResults)) {
            return;
        }

        Map<String, String> added = new HashMap<>();
        List<String> addedHeaders = response.getStringHeaders().remove("X-Trino-Added-Prepare");
        if (addedHeaders != null) {
            for (String header : addedHeaders) {
                List<String> split = Splitter.on("=").splitToList(header);
                added.put(decode(split.get(0), UTF_8), decode(split.get(1), UTF_8));
            }
        }

        Set<String> deallocated = new HashSet<>();
        List<String> deallocatedHeaders = response.getStringHeaders().remove("X-Trino-Deallocated-Prepare");
        if (deallocatedHeaders != null) {
            for (String header : deallocatedHeaders) {
                deallocated.add(decode(header, UTF_8));
            }
        }

        QueryResults queryResults = (QueryResults) response.getEntity();
        response.setEntity(new ExtendedQueryResults(queryResults, added, deallocated));
    }

    public static class ExtendedQueryResults
    {
        private final QueryResults results;
        private final Map<String, String> addedPreparedStatements;
        private final Set<String> deallocatedPreparedStatements;

        public ExtendedQueryResults(QueryResults results, Map<String, String> addedPreparedStatements, Set<String> deallocatedPreparedStatements)
        {
            this.results = requireNonNull(results, "results is null");
            this.addedPreparedStatements = ImmutableMap.copyOf(requireNonNull(addedPreparedStatements, "addedPreparedStatements is null"));
            this.deallocatedPreparedStatements = ImmutableSet.copyOf(requireNonNull(deallocatedPreparedStatements, "deallocatedPreparedStatements is null"));
        }

        @JsonUnwrapped
        @JsonProperty
        public QueryResults getResults()
        {
            return results;
        }

        @JsonProperty
        public Map<String, String> getAddedPreparedStatements()
        {
            return addedPreparedStatements;
        }

        @JsonProperty
        public Set<String> getDeallocatedPreparedStatements()
        {
            return deallocatedPreparedStatements;
        }
    }
}
