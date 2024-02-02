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

package io.trino.server.resultscache;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.starburst.stargate.resultscache.client.CacheClient;
import io.starburst.stargate.resultscache.model.CacheEntry;
import io.trino.client.Column;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.server.security.galaxy.GalaxyIdentity.toDispatchSession;
import static java.util.Objects.requireNonNull;

public class ResultsCacheClient
{
    private static final Logger log = Logger.get(ResultsCacheClient.class);
    private static final String AUTH_TOKEN_TYPE = "X-Trino-Plane-Token";
    private final String cacheBaseUri;
    private final CacheClient cacheClient;
    private final boolean galaxyEnabled;
    private final Optional<String> clusterId;
    private final Optional<String> deploymentId;

    public ResultsCacheClient(String cacheBaseUri, HttpClient httpClient, boolean galaxyEnabled, Optional<String> clusterId, Optional<String> deploymentId)
    {
        this.cacheBaseUri = requireNonNull(cacheBaseUri, "cacheBaseUri is null");
        this.cacheClient = new CacheClient(requireNonNull(httpClient, "httpClient is null"));
        this.galaxyEnabled = galaxyEnabled;
        this.clusterId = requireNonNull(clusterId, "clusterId is null");
        this.deploymentId = requireNonNull(deploymentId, "deploymentId is null");
    }

    public void uploadResultsCacheEntry(
            Identity identity,
            String key,
            QueryId queryId,
            String query,
            Optional<String> sessionCatalog,
            Optional<String> sessionSchema,
            Optional<String> queryType,
            Optional<String> updateType,
            List<Column> columns,
            List<List<Object>> data,
            Instant creation,
            Optional<String> principal)
    {
        if (!galaxyEnabled) {
            log.debug("Galaxy not enabled. Skip sending cache entry %s for query %s to results cache", key, queryId);
            return;
        }

        log.debug("Sending cache entry %s for query %s to results cache", key, queryId);
        try {
            cacheClient.insertCacheEntry(
                    cacheBaseUri,
                    getTokenSupplier(identity),
                    createCacheEntry(key, queryId, query, sessionCatalog, sessionSchema, queryType, updateType, clusterId.get(), deploymentId.get(), columns, data, creation, principal));
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException("Error serializing results to JSON", ex);
        }
    }

    private static Supplier<String> getTokenSupplier(Identity identity)
    {
        return () -> AUTH_TOKEN_TYPE + " " + toDispatchSession(identity).getAccessToken();
    }

    private static CacheEntry createCacheEntry(
            String cacheKey,
            QueryId queryId,
            String query,
            Optional<String> sessionCatalog,
            Optional<String> sessionSchema,
            Optional<String> queryType,
            Optional<String> updateType,
            String clusterId,
            String deploymentId,
            List<Column> columns,
            List<List<Object>> data,
            Instant creation,
            Optional<String> princpal)
            throws JsonProcessingException
    {
        return new CacheEntry(
                cacheKey,
                queryId.toString(),
                query,
                sessionCatalog,
                sessionSchema,
                queryType,
                updateType,
                sessionCatalog.stream().toList(),
                clusterId,
                deploymentId,
                columns,
                data,
                creation,
                princpal);
    }
}
