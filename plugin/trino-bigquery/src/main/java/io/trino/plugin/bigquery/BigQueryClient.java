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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.base.cache.EvictableCache;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.TableNotFoundException;

import javax.swing.text.html.Option;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.google.cloud.bigquery.TableDefinition.Type.TABLE;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_AMBIGUOUS_OBJECT_NAME;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.weakref.jmx.internal.guava.base.Throwables.throwIfInstanceOf;

public class BigQueryClient
{
    private static final Logger log = Logger.get(BigQueryClient.class);

    private final BigQuery bigQuery;
    private final ViewMaterializationCache materializationCache;
    private final boolean caseInsensitiveNameMatching;
    // projectId -> [lowercase] datasetName -> RemoteDatabaseObject
    private final EvictableCache<String, Map<String, Optional<RemoteDatabaseObject>>> remoteDatasets;
    // Entry<projectId, datasetName> -> [lowercase] tableName -> RemoteDatabaseObject
    private final EvictableCache<Map.Entry<String, String>, Map<String, Optional<RemoteDatabaseObject>>> remoteTables;

    public BigQueryClient(BigQuery bigQuery, BigQueryConfig config, ViewMaterializationCache materializationCache)
    {
        this.bigQuery = requireNonNull(bigQuery, "bigQuery is null");
        this.materializationCache = requireNonNull(materializationCache, "materializationCache is null");

        Duration caseInsensitiveNameMatchingCacheTtl = requireNonNull(config.getCaseInsensitiveNameMatchingCacheTtl(), "caseInsensitiveNameMatchingCacheTtl is null");

        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
        CacheBuilder<Object, Object> remoteNamesCacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(caseInsensitiveNameMatchingCacheTtl.toMillis(), MILLISECONDS);
        this.remoteDatasets = EvictableCache.buildWith(remoteNamesCacheBuilder);
        this.remoteTables = EvictableCache.buildWith(remoteNamesCacheBuilder);
    }

    public Optional<RemoteDatabaseObject> toRemoteDataset(String projectId, String datasetName)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(datasetName, "datasetName is null");
        verify(datasetName.codePoints().noneMatch(Character::isUpperCase), "Expected schema name from internal metadata to be lowercase: %s", datasetName);
        if (!caseInsensitiveNameMatching) {
            return Optional.of(RemoteDatabaseObject.of(datasetName));
        }


        // Check if we have a cached result, if we do and the cached result doesn't contain the datasetName then invalidate
        Map<String, Optional<RemoteDatabaseObject>> cachedDatasetToRemoteDatabaseObject = remoteDatasets.getIfPresent(projectId);
        if (cachedDatasetToRemoteDatabaseObject != null && !cachedDatasetToRemoteDatabaseObject.containsKey(datasetName)) {
            remoteDatasets.invalidate(projectId);
        }

        Map<String, Optional<RemoteDatabaseObject>> datasetToRemoteDatabaseObject = get(remoteDatasets, projectId, () -> {
            // cache miss, list all the datasets and refresh cache
            Map<String, Optional<RemoteDatabaseObject>> mapping = new HashMap<>();
            for (Dataset dataset : listDatasets(projectId)) {
                mapping.merge(
                        dataset.getDatasetId().getDataset().toLowerCase(ENGLISH),
                        Optional.of(RemoteDatabaseObject.of(dataset.getDatasetId().getDataset())),
                        (currentValue, collision) -> Optional.of(currentValue.get().registerCollision(collision.get().getOnlyRemoteName())));
            }
            // explicitly cache the negative entry if the requested dataset doesn't exist
            if (!mapping.containsKey(datasetName)) {
                mapping.put(datasetName, Optional.empty());
            }
            return mapping;
        });

        Optional<RemoteDatabaseObject> remoteDatabaseObject = datasetToRemoteDatabaseObject.get(datasetName);
        return remoteDatabaseObject == null ? Optional.empty() : remoteDatabaseObject;

    }

    public Optional<RemoteDatabaseObject> toRemoteTable(String projectId, String remoteDatasetName, String tableName)
    {
        return toRemoteTable(projectId, remoteDatasetName, tableName, () -> listTables(DatasetId.of(projectId, remoteDatasetName), TABLE, VIEW));
    }

    public Optional<RemoteDatabaseObject> toRemoteTable(String projectId, String remoteDatasetName, String tableName, Iterable<Table> tables)
    {
        return toRemoteTable(projectId, remoteDatasetName, tableName, () -> tables);
    }

    private Optional<RemoteDatabaseObject> toRemoteTable(String projectId, String remoteDatasetName, String tableName, Supplier<Iterable<Table>> tables)
    {
        requireNonNull(projectId, "projectId is null");
        requireNonNull(remoteDatasetName, "remoteDatasetName is null");
        requireNonNull(tableName, "tableName is null");
        verify(tableName.codePoints().noneMatch(Character::isUpperCase), "Expected table name from internal metadata to be lowercase: %s", tableName);
        if (!caseInsensitiveNameMatching) {
            return Optional.of(RemoteDatabaseObject.of(tableName));
        }

        Map.Entry<String, String> cacheKey = Map.entry(projectId, remoteDatasetName);

        // Check if we have a cached result, if we do and the cached result doesn't contain the tableName then invalidate
        Map<String, Optional<RemoteDatabaseObject>> cachedTableToRemoteDatabaseObject = remoteTables.getIfPresent(cacheKey);
        if (cachedTableToRemoteDatabaseObject != null && !cachedTableToRemoteDatabaseObject.containsKey(tableName)) {
            remoteDatasets.invalidate(projectId);
        }

        Map<String, Optional<RemoteDatabaseObject>> tableToRemoteDatabaseObject = get(remoteTables, cacheKey, () -> {
            // cache miss, list all the tables and refresh cache
            Map<String, Optional<RemoteDatabaseObject>> mapping = new HashMap<>();
            for (Table table : tables.get()) {
                mapping.merge(
                        table.getTableId().getTable().toLowerCase(ENGLISH),
                        Optional.of(RemoteDatabaseObject.of(table.getTableId().getTable())),
                        (currentValue, collision) -> Optional.of(currentValue.get().registerCollision(collision.get().getOnlyRemoteName())));
            }
            // explicitly cache the negative entry if the requested tableName doesn't exist
            if (!mapping.containsKey(tableName)) {
                mapping.put(tableName, Optional.empty());
            }
            return mapping;
        });

        Optional<RemoteDatabaseObject> remoteDatabaseObject = tableToRemoteDatabaseObject.get(tableName);
        return remoteDatabaseObject == null ? Optional.empty() : remoteDatabaseObject;
    }

    private static TableId tableIdToLowerCase(TableId tableId)
    {
        return TableId.of(
                tableId.getProject(),
                tableId.getDataset(),
                tableId.getTable().toLowerCase(ENGLISH));
    }

    public DatasetInfo getDataset(DatasetId datasetId)
    {
        return bigQuery.getDataset(datasetId);
    }

    public Optional<TableInfo> getTable(TableId remoteTableId)
    {
        return Optional.ofNullable(bigQuery.getTable(remoteTableId));
    }

    public TableInfo getCachedTable(Duration viewExpiration, TableInfo remoteTableId, List<String> requiredColumns)
    {
        String query = selectSql(remoteTableId, requiredColumns);
        log.debug("query is %s", query);
        return materializationCache.getCachedTable(this, query, viewExpiration, remoteTableId);
    }

    public String getProjectId()
    {
        return bigQuery.getOptions().getProjectId();
    }

    public Iterable<Dataset> listDatasets(String projectId)
    {
        return bigQuery.listDatasets(projectId).iterateAll();
    }

    public Iterable<Table> listTables(DatasetId remoteDatasetId, TableDefinition.Type... types)
    {
        Set<TableDefinition.Type> allowedTypes = ImmutableSet.copyOf(types);
        Iterable<Table> allTables = bigQuery.listTables(remoteDatasetId).iterateAll();
        return stream(allTables)
                .filter(table -> allowedTypes.contains(table.getDefinition().getType()))
                .collect(toImmutableList());
    }

    Table update(TableInfo table)
    {
        return bigQuery.update(table);
    }

    public void createSchema(DatasetInfo datasetInfo)
    {
        bigQuery.create(datasetInfo);
    }

    public void dropSchema(DatasetId datasetId)
    {
        bigQuery.delete(datasetId);
        remoteDatasets.invalidate(getProjectId());
    }

    public void createTable(TableInfo tableInfo)
    {
        bigQuery.create(tableInfo);
    }

    public void dropTable(TableId tableId)
    {
        bigQuery.delete(tableId);
        remoteTables.invalidate(Map.entry(getProjectId(), tableId.getDataset()));
    }

    Job create(JobInfo jobInfo)
    {
        return bigQuery.create(jobInfo);
    }

    public TableResult query(String sql)
    {
        try {
            return bigQuery.query(QueryJobConfiguration.of(sql));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryException(BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the query [%s]", sql), e);
        }
    }

    private String selectSql(TableInfo remoteTable, List<String> requiredColumns)
    {
        String columns = requiredColumns.isEmpty() ? "*" :
                requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

        return selectSql(remoteTable.getTableId(), columns);
    }

    // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and SUM
    public String selectSql(TableId table, String formattedColumns)
    {
        String tableName = fullTableName(table);
        return format("SELECT %s FROM `%s`", formattedColumns, tableName);
    }

    private String fullTableName(TableId remoteTableId)
    {
        String remoteSchemaName = remoteTableId.getDataset();
        String remoteTableName = remoteTableId.getTable();
        remoteTableId = TableId.of(remoteTableId.getProject(), remoteSchemaName, remoteTableName);
        return format("%s.%s.%s", remoteTableId.getProject(), remoteTableId.getDataset(), remoteTableId.getTable());
    }

    public List<BigQueryColumnHandle> getColumns(BigQueryTableHandle tableHandle)
    {
        TableInfo tableInfo = getTable(tableHandle.getRemoteTableName().toTableId())
                .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
        Schema schema = tableInfo.getDefinition().getSchema();
        if (schema == null) {
            throw new TableNotFoundException(
                    tableHandle.getSchemaTableName(),
                    format("Table '%s' has no schema", tableHandle.getSchemaTableName()));
        }
        return schema.getFields()
                .stream()
                .filter(Conversions::isSupportedType)
                .map(Conversions::toColumnHandle)
                .collect(toImmutableList());
    }

    public static final class RemoteDatabaseObject
    {
        private final Set<String> remoteNames;

        private RemoteDatabaseObject(Set<String> remoteNames)
        {
            this.remoteNames = ImmutableSet.copyOf(remoteNames);
        }

        public static RemoteDatabaseObject of(String remoteName)
        {
            return new RemoteDatabaseObject(ImmutableSet.of(remoteName));
        }

        public RemoteDatabaseObject registerCollision(String ambiguousName)
        {
            return new RemoteDatabaseObject(ImmutableSet.<String>builderWithExpectedSize(remoteNames.size() + 1)
                    .addAll(remoteNames)
                    .add(ambiguousName)
                    .build());
        }

        public String getAnyRemoteName()
        {
            return Collections.min(remoteNames);
        }

        public String getOnlyRemoteName()
        {
            if (!isAmbiguous()) {
                return getOnlyElement(remoteNames);
            }

            throw new TrinoException(BIGQUERY_AMBIGUOUS_OBJECT_NAME, "Found ambiguous names in BigQuery when looking up '" + getAnyRemoteName().toLowerCase(ENGLISH) + "': " + remoteNames);
        }

        public boolean isAmbiguous()
        {
            return remoteNames.size() > 1;
        }
    }

    private static <K, V> V get(EvictableCache<K, V> cache, K key, Callable<V> loader)
    {
        try {
            return cache.get(key, loader);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new UncheckedExecutionException(e);
        }
    }
}
