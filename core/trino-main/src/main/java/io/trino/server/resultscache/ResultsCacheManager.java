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
import io.airlift.units.DataSize;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ResultsCacheManager
{
    private static final long DEFAULT_MAXIMUM_SIZE_BYTES = DataSize.of(1, MEGABYTE).toBytes();

    private final ResultsCacheClient resultsCacheClient;
    private final ListeningExecutorService executorService;

    @Inject
    public ResultsCacheManager(@ForResultsCache HttpClient httpClient, ResultsCacheConfig config)
    {
        this.executorService = listeningDecorator(newFixedThreadPool(config.getCacheUploadThreads(), threadsNamed("resultscache-upload-%s")));
        this.resultsCacheClient = new ResultsCacheClient(
                config.getCacheEndpoint(),
                requireNonNull(httpClient, "httpClient is null"));
    }

    public ResultsCacheEntry createResultsCacheEntry(Identity identity, ResultsCacheParameters resultsCacheParameters, QueryId queryId)
    {
        long maximumSizeBytes = resultsCacheParameters.maximumSizeBytes().orElse(DEFAULT_MAXIMUM_SIZE_BYTES);
        return new ResultsCacheEntry(
                identity,
                resultsCacheParameters.key(),
                queryId,
                maximumSizeBytes,
                resultsCacheParameters.ttl(),
                resultsCacheClient,
                executorService);
    }
}
