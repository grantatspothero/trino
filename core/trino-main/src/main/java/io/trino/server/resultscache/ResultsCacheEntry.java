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
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.client.Column;
import io.trino.spi.QueryId;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ResultsCacheEntry
{
    private static final Logger log = Logger.get(ResultsCacheEntry.class);
    private static final long MAX_SIZE = 16_777_216;
    private final String key;
    private final QueryId queryId;
    private final List<String> catalogs;
    private final List<String> schemas;
    private final List<String> tables;
    private final long maximumSize;
    private final Duration expirationInterval;
    private final ResultsCacheClient client;
    private long nextToken;
    private long currentSize;
    private boolean valid = true;
    private Optional<ResultsData> resultsData = Optional.empty();

    public ResultsCacheEntry(
            String key,
            QueryId queryId,
            List<String> catalogs,
            List<String> schemas,
            List<String> tables,
            long maximumSize,
            Duration expirationInterval,
            ResultsCacheClient client)
    {
        this.key = requireNonNull(key, "key is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.catalogs = ImmutableList.copyOf(requireNonNull(catalogs, "catalogs is null"));
        this.schemas = ImmutableList.copyOf(requireNonNull(schemas, "schemas is null"));
        this.tables = ImmutableList.copyOf(requireNonNull(tables, "tables is null"));
        checkState(maximumSize > 0, "maximumSize is <= 0");
        if (maximumSize > MAX_SIZE) {
            log.warn("Cache entry size: %s is greater than the maximum: %s, using maximum", maximumSize, MAX_SIZE);
        }
        this.maximumSize = Math.min(maximumSize, MAX_SIZE);
        this.expirationInterval = requireNonNull(expirationInterval, "expirationInterval is null");
        this.client = requireNonNull(client, "client is null");
        checkState(!catalogs.isEmpty(), "catalogs is empty");
        checkState(!schemas.isEmpty(), "schemas is empty");
        checkState(!tables.isEmpty(), "tables is empty");
    }

    public void consume(long token, List<Column> columns, Iterable<List<Object>> data, long logicalSizeInBytes)
    {
        if (!valid) {
            return;
        }

        checkState(token == nextToken, format("Invalid token %d, expected %d", token, nextToken));
        checkState(token == 0 || resultsData.isPresent(), "resultsData in invalid state");

        if (token == 0) {
            resultsData = Optional.of(new ResultsData(columns));
        }
        ++nextToken;

        currentSize += logicalSizeInBytes;

        if (currentSize > maximumSize) {
            valid = false;
            resultsData = Optional.empty();
        }
        else {
            resultsData.get().addRecords(data);
        }
    }

    public void done()
    {
        if (valid) {
            log.debug("Sending cache entry %s for query %s to results cache", key, queryId);
            client.uploadResultsCacheEntry(key, queryId, catalogs, schemas, tables, resultsData.get().columns, resultsData.get().data, expirationInterval);
        }
        valid = false;
    }

    private static class ResultsData
    {
        private final List<Column> columns;
        private List<List<Object>> data = new ArrayList<>();

        public ResultsData(List<Column> columns)
        {
            this.columns = requireNonNull(columns, "columns is null");
        }

        public void addRecords(Iterable<List<Object>> records)
        {
            Iterables.addAll(data, records);
        }
    }
}
