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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.server.protocol.QueryResultRows;
import io.trino.server.protocol.data.InlineJsonQueryDataProducer;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.security.Identity;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.CACHED;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.CACHING;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.NO_COLUMNS;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.OVER_MAX_SIZE;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static java.util.Objects.requireNonNull;

public class ActiveResultsCacheEntry
        implements ResultsCacheEntry
{
    private static final Logger log = Logger.get(ActiveResultsCacheEntry.class);
    private final Identity identity;
    private final String cacheKey;
    private final QueryId queryId;
    private final String query;
    private final Optional<String> sessionCatalog;
    private final Optional<String> sessionSchema;
    private final Optional<String> queryType;
    private final Optional<String> updateType;
    private final long maximumSize;
    private final ResultsCacheClient client;
    private final ListeningExecutorService executorService;
    private final Optional<String> principal;
    @GuardedBy("this")
    private long currentSize;
    @GuardedBy("this")
    private boolean doneCalled;
    @GuardedBy("this")
    private boolean valid = true;
    @GuardedBy("this")
    private Optional<ResultsData> resultsData = Optional.empty();
    @GuardedBy("this")
    private ResultsCacheResult entryResult;
    @GuardedBy("this")
    private List<DoneCallback> doneCallbacks = new ArrayList<>();

    public ActiveResultsCacheEntry(
            Identity identity,
            String cacheKey,
            QueryId queryId,
            String query,
            Optional<String> sessionCatalog,
            Optional<String> sessionSchema,
            Optional<String> queryType,
            Optional<String> updateType,
            long maximumSize,
            ResultsCacheClient client,
            ListeningExecutorService executorService,
            Optional<String> principal)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.cacheKey = requireNonNull(cacheKey, "key is null");
        this.entryResult = new ResultsCacheResult(CACHING);
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.query = requireNonNull(query, "query is null");
        this.sessionCatalog = requireNonNull(sessionCatalog, "sessionCatalog is null");
        this.sessionSchema = requireNonNull(sessionSchema, "sessionSchema is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.updateType = requireNonNull(updateType, "updateType is null");
        checkArgument(maximumSize > 0, "maximumSize is <= 0");
        this.maximumSize = maximumSize;
        this.client = requireNonNull(client, "client is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.principal = requireNonNull(principal, "principal is null");
    }

    @Override
    public void done()
    {
        List<CompletionCallback> completionActions = new ArrayList<>();
        synchronized (this) {
            if (!doneCalled) {
                if (valid) {
                    if (resultsData.isPresent()) {
                        log.debug("QueryId: %s, done called and cache entry is valid, uploading to cache", queryId);
                        entryResult = new ResultsCacheResult(CACHED, currentSize);
                        submitAsyncUpload(OffsetDateTime.now().toInstant(), resultsData.orElseThrow());
                        resultsData = Optional.empty();
                    }
                    else {
                        log.debug("QueryId: %s, done called and cache entry is empty.  Not uploading", queryId);
                    }
                    completionActions.add(() -> doneCallbacks.forEach(DoneCallback::done));
                }
                doneCalled = true;
            }
        }
        completionActions.forEach(CompletionCallback::apply);
    }

    @Override
    public boolean isDone()
    {
        return doneCalled || !valid;
    }

    @Override
    public boolean addTransitionToDoneCallback(DoneCallback doneCallback)
    {
        synchronized (this) {
            if (isDone()) {
                return false;
            }
            doneCallbacks.add(doneCallback);
        }
        return true;
    }

    @Override
    public Optional<ResultsCacheResult> getEntryResult()
    {
        return Optional.of(entryResult);
    }

    public void appendResults(Session session, List<Column> columns, QueryResultRows resultRows)
    {
        InlineJsonQueryDataProducer queryDataProducer = new InlineJsonQueryDataProducer();
        List<CompletionCallback> completionCallbacks = new ArrayList<>();
        try {
            synchronized (this) {
                if (doneCalled && !resultRows.isEmpty()) {
                    throw new IllegalStateException("result rows added after done method called");
                }

                if (isDone()) {
                    return;
                }

                if (resultsData.isEmpty()) {
                    if (columns == null && resultRows.isEmpty()) {
                        log.debug("QueryId: %s, received null columns and empty rows for cache entry %s, ignoring", queryId, cacheKey);
                        return;
                    }
                    else if (columns == null) {
                        log.debug("QueryId: %s, null columns provided for results cache entry %s, not caching", queryId, cacheKey);
                        completionCallbacks.add(setInvalidState(NO_COLUMNS));
                        return;
                    }

                    resultsData = Optional.of(new ResultsData(columns));
                }

                long logicalSizeInBytes = resultRows.countLogicalSizeInBytes();
                currentSize += logicalSizeInBytes;
                if (logicalSizeInBytes > 0) {
                    entryResult = entryResult.withCurrentSize(currentSize);
                }

                if (currentSize > maximumSize) {
                    log.debug("QueryId: %s, results exceeded maximum size of %s bytes, not caching", queryId, maximumSize);
                    completionCallbacks.add(setInvalidState(OVER_MAX_SIZE));
                    return;
                }

                log.debug("QueryId: %s, appending to cache entry, %s bytes, %s current total size", queryId, logicalSizeInBytes, currentSize);
                QueryData queryData = queryDataProducer.produce(session, resultRows, true, throwable -> {
                    throw new TrinoException(SERIALIZATION_ERROR, "Error converting output to client protocol", throwable);
                });
                resultsData.get().addRecords(queryData.getData());
            }
        }
        finally {
            completionCallbacks.forEach(CompletionCallback::apply);
        }
    }

    @GuardedBy("this")
    private CompletionCallback setInvalidState(ResultsCacheResult.Status invalidState)
    {
        checkState(valid, "invalid state already set");

        valid = false;
        entryResult = new ResultsCacheResult(invalidState);
        return () -> doneCallbacks.forEach(DoneCallback::done);
    }

    @GuardedBy("this")
    private void submitAsyncUpload(Instant createdTime, ResultsData resultsData)
    {
        checkState(valid, "attempting to upload results in invalid state");

        ListenableFuture<?> submitFuture = executorService.submit(() ->
                client.uploadResultsCacheEntry(
                        identity,
                        cacheKey,
                        queryId,
                        query,
                        sessionCatalog,
                        sessionSchema,
                        queryType,
                        updateType,
                        resultsData.columns,
                        resultsData.data,
                        createdTime,
                        principal));
        MoreFutures.addExceptionCallback(submitFuture, throwable ->
                log.error(throwable, "Upload to cache failed"));
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

    private interface CompletionCallback
    {
        void apply();
    }
}
