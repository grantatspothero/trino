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

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import static io.trino.plugin.objectstore.FeatureExposure.EXPOSED;
import static io.trino.plugin.objectstore.FeatureExposure.HIDDEN;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.ICEBERG;

public final class FeatureExposures
{
    private FeatureExposures() {}

    public static Table<TableType, String, FeatureExposure> procedureExposureDecisions()
    {
        return ImmutableTable.<TableType, String, FeatureExposure>builder()
                // sorted by procedure name, then by connector in (Hive, Iceberg, Delta, Hudi) order
                .put(HIVE, "create_empty_partition", EXPOSED)
                .put(HIVE, "drop_stats", EXPOSED) // TODO similar to drop_extended_stats exposed by Delta
                .put(DELTA, "drop_extended_stats", EXPOSED) // TODO similar to drop_stats exposed by hive
                .put(HIVE, "flush_metadata_cache", HIDDEN) // see ObjectStoreFlushMetadataCache
                .put(DELTA, "flush_metadata_cache", HIDDEN) // see ObjectStoreFlushMetadataCache
                .put(ICEBERG, "migrate", HIDDEN) // supported via ALTER TABLE SET PROPERTIES syntax
                .put(HIVE, "register_partition", EXPOSED)
                .put(DELTA, "register_table", HIDDEN) // see ObjectStoreRegisterTableProcedure
                .put(ICEBERG, "register_table", HIDDEN) // see ObjectStoreRegisterTableProcedure
                .put(ICEBERG, "rollback_to_snapshot", EXPOSED)
                .put(HIVE, "sync_partition_metadata", EXPOSED)
                .put(HIVE, "unregister_partition", EXPOSED)
                .put(DELTA, "unregister_table", HIDDEN) // see ObjectStoreUnregisterTableProcedure
                .put(ICEBERG, "unregister_table", HIDDEN) // see ObjectStoreUnregisterTableProcedure
                .put(DELTA, "vacuum", EXPOSED)
                .buildOrThrow();
    }
}
