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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.client.Column;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class ResultsCacheEntry
{
    private static final Logger log = Logger.get(ResultsCacheEntry.class);
    private static final long MAX_SIZE = DataSize.of(1, MEGABYTE).toBytes();
    private final Identity identity;
    private final String key;
    private final QueryId queryId;
    private final long maximumSize;
    private final Duration expirationInterval;
    private final ResultsCacheClient client;
    private final ListeningExecutorService executorService;
    private long currentSize;
    private boolean valid = true;
    private Optional<ResultsData> resultsData = Optional.empty();

    public ResultsCacheEntry(
            Identity identity,
            String key,
            QueryId queryId,
            long maximumSize,
            Duration expirationInterval,
            ResultsCacheClient client,
            ListeningExecutorService executorService)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.key = requireNonNull(key, "key is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        checkArgument(maximumSize > 0, "maximumSize is <= 0");
        if (maximumSize > MAX_SIZE) {
            log.warn("Cache entry size: %s is greater than the maximum: %s, using maximum", maximumSize, MAX_SIZE);
        }
        this.maximumSize = Math.min(maximumSize, MAX_SIZE);
        this.expirationInterval = requireNonNull(expirationInterval, "ttl is null");
        this.client = requireNonNull(client, "client is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    public void appendResults(List<Column> columns, Iterable<List<Object>> data, long logicalSizeInBytes)
    {
        if (!valid) {
            return;
        }

        currentSize += logicalSizeInBytes;

        if (currentSize > maximumSize) {
            valid = false;
            resultsData = Optional.empty();
            return;
        }

        if (resultsData.isEmpty()) {
            resultsData = Optional.of(new ResultsData(columns));
        }
        resultsData.get().addRecords(data);
    }

    public void done()
    {
        if (valid) {
            submitAsyncUpload(executorService, client, identity, key, queryId, expirationInterval, resultsData.orElseThrow());
            resultsData = Optional.empty();
        }
        valid = false;
    }

    private static void submitAsyncUpload(
            ListeningExecutorService executorService,
            ResultsCacheClient client,
            Identity identity,
            String cacheKey,
            QueryId queryId,
            Duration expirationInterval,
            ResultsData resultsData)
    {
        ListenableFuture<?> submitFuture = executorService.submit(() ->
                client.uploadResultsCacheEntry(identity, cacheKey, queryId, resultsData.columns, resultsData.data, expirationInterval));

        MoreFutures.addExceptionCallback(submitFuture, throwable ->
                log.error("Upload to cache failed: %s", throwable));
    }

    private static class ResultsData
    {
        private final List<Column> columns;
        private List<List<Object>> data = new ArrayList<>();

        public ResultsData(List<Column> columns)
        {
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        }

        public void addRecords(Iterable<List<Object>> records)
        {
            Iterables.addAll(data, records);
        }
    }
}
