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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.http.client.HttpClient;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.resultscache.client.CacheClient;
import io.starburst.stargate.resultscache.model.CacheEntry;
import io.trino.client.Column;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.security.galaxy.GalaxyIdentity.toDispatchSession;
import static java.util.Objects.requireNonNull;

public class ResultsCacheClient
{
    private static final Logger log = Logger.get(ResultsCacheClient.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private final String cacheBaseUri;
    private final CacheClient cacheClient;

    public ResultsCacheClient(String cacheBaseUri, HttpClient httpClient)
    {
        this.cacheBaseUri = requireNonNull(cacheBaseUri, "cacheBaseUri is null");
        this.cacheClient = new CacheClient(requireNonNull(httpClient, "httpClient is null"));
    }

    public void uploadResultsCacheEntry(
            Identity identity,
            String key,
            QueryId queryId,
            List<Column> columns,
            List<List<Object>> data,
            Duration expirationInterval)
    {
        log.debug("Sending cache entry %s for query %s to results cache", key, queryId);
        try {
            cacheClient.insertCacheEntry(
                    cacheBaseUri,
                    getTokenSupplier(identity),
                    createCacheEntry(key, queryId, columns, data, expirationInterval));
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException("Error serializing results to JSON", ex);
        }
    }

    private static Supplier<String> getTokenSupplier(Identity identity)
    {
        return () -> toDispatchSession(identity).getAccessToken();
    }

    private static CacheEntry createCacheEntry(
            String cacheKey,
            QueryId queryId,
            List<Column> columns,
            List<List<Object>> data,
            Duration expirationInterval)
            throws JsonProcessingException
    {
        Instant expirationTime = Instant.now().plusMillis(expirationInterval.toMillis());
        List<List<String>> dataStr = data.stream().map(singleRow -> singleRow.stream().map(ResultsCacheClient::serializeObject).collect(toImmutableList())).collect(toImmutableList());
        return new CacheEntry(cacheKey, queryId.toString(), columns, dataStr, expirationTime);
    }

    private static String serializeObject(Object object)
    {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException("Error serializing results to JSON", ex);
        }
    }
}
