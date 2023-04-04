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
package io.trino.plugin.objectstore;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.SchemaTableName;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import javax.annotation.concurrent.ThreadSafe;

import java.util.EnumMap;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static java.util.concurrent.TimeUnit.HOURS;

@ThreadSafe
public final class TableTypeCache
{
    private static final TableType DEFAULT_TABLE = ICEBERG;
    private static final List<TableType> DEFAULT_TABLE_TYPE_ORDER = ImmutableList.of(ICEBERG, DELTA, HUDI, HIVE);
    private static final EnumMap<TableType, List<TableType>> ORDER_BY_CACHED_TYPE;

    static {
        ORDER_BY_CACHED_TYPE = new EnumMap<>(TableType.class);
        for (TableType firstType : DEFAULT_TABLE_TYPE_ORDER) {
            ImmutableList.Builder<TableType> order = ImmutableList.builderWithExpectedSize(DEFAULT_TABLE_TYPE_ORDER.size());
            order.add(firstType);
            for (TableType type : DEFAULT_TABLE_TYPE_ORDER) {
                if (type != firstType) {
                    order.add(type);
                }
            }
            ORDER_BY_CACHED_TYPE.put(firstType, order.build());
        }
    }

    private final Cache<SchemaTableName, TableType> cache;

    public TableTypeCache()
    {
        // it is acceptable for this cache to sometimes return stale value,
        // it is only used to decide about order of calls, so it doesn't affect correctness
        // for the same reason there is no invalidation logic or any other similar mechanism enabled
        this.cache = buildUnsafeCacheWithInvalidationRace(CacheBuilder.newBuilder()
                .expireAfterWrite(1, HOURS)
                .maximumSize(1000));
    }

    @SuppressModernizer
    private static <K, V> Cache<K, V> buildUnsafeCacheWithInvalidationRace(CacheBuilder<? super K, ? super V> cacheBuilder)
    {
        return cacheBuilder.build();
    }

    public List<TableType> getTableTypeAffinity(SchemaTableName tableName)
    {
        TableType tableType = firstNonNull(cache.getIfPresent(tableName), DEFAULT_TABLE);
        return ORDER_BY_CACHED_TYPE.get(tableType);
    }

    public void update(SchemaTableName tableName, TableType currentTableType)
    {
        // it is acceptable for this cache to have a stale value
        cache.put(tableName, currentTableType);
    }
}
