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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;

import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.server.resultscache.ResultsCacheSessionProperties.getResultsCacheKey;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ResultsCacheManager
{
    private static final Logger log = Logger.get(ResultsCacheManager.class);
    private final long configuredMaxSize;
    private final ResultsCacheClient resultsCacheClient;
    private final ListeningExecutorService executorService;

    @Inject
    public ResultsCacheManager(@ForResultsCache HttpClient httpClient, ResultsCacheConfig config)
    {
        this.configuredMaxSize = config.getMaxResultsSize().toBytes();
        this.resultsCacheClient = new ResultsCacheClient(
                config.getCacheEndpoint(),
                requireNonNull(httpClient, "httpClient is null"),
                config.isGalaxyEnabled(),
                config.getClusterId(),
                config.getDeploymentId());
        this.executorService = listeningDecorator(newFixedThreadPool(config.getCacheUploadThreads(), threadsNamed("resultscache-upload-%s")));
    }

    public ActiveResultsCacheEntry createResultsCacheEntry(
            Identity identity,
            ResultsCacheState resultsCacheParameters,
            QueryId queryId,
            String query,
            Optional<String> sessionCatalog,
            Optional<String> sessionSchema,
            Optional<String> queryType,
            Optional<String> updateType,
            Optional<String> principal)
    {
        Optional<Long> requestMaxSizeOptional = resultsCacheParameters.maximumSizeBytes();
        long maximumSizeBytes = requestMaxSizeOptional.map(requestMaxSize -> {
            if (requestMaxSize > configuredMaxSize) {
                log.debug("Maximum results size configured in request %s is larger than globally configured maximum %s.", requestMaxSize, configuredMaxSize);
                return configuredMaxSize;
            }
            return requestMaxSize;
        }).orElse(configuredMaxSize);

        log.debug("QueryId: %s, created ResultsCacheEntry with key %s", queryId, resultsCacheParameters.key());
        return new ActiveResultsCacheEntry(
                identity,
                resultsCacheParameters.key(),
                queryId,
                query,
                sessionCatalog,
                sessionSchema,
                queryType,
                updateType,
                maximumSizeBytes,
                resultsCacheClient,
                executorService,
                principal);
    }

    public static Optional<ResultsCacheState> createResultsCacheParameters(Session session)
    {
        return getResultsCacheKey(session).map(cacheKey -> {
            log.debug("QueryId: %s, statement had cache key %s", session.getQueryId(), cacheKey);
            return new ResultsCacheState(
                    cacheKey,
                    ResultsCacheSessionProperties.getResultsCacheEntryMaxSizeBytes(session)); });
    }
}
