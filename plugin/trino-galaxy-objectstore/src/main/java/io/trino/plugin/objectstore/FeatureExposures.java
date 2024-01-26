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
import static io.trino.plugin.objectstore.FeatureExposure.INACCESSIBLE;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;

public final class FeatureExposures
{
    private FeatureExposures() {}

    public static Table<TableType, String, FeatureExposure> sessionExposureDecisions()
    {
        return ImmutableTable.<TableType, String, FeatureExposure>builder()
                // sorted by connector in (Hive, Iceberg, Delta, Hudi) order, then by session property name
                // TODO Note the map may contain entries for session properties no longer exposed by delegate connectors. The set of session properties is config-dependent which makes detection hard.
                .put(HIVE, "bucket_execution_enabled", EXPOSED)
                .put(HIVE, "collect_column_statistics_on_write", EXPOSED)
                .put(HIVE, "compression_codec", EXPOSED)
                .put(HIVE, "create_empty_bucket_files", EXPOSED)
                .put(HIVE, "delegate_transactional_managed_table_location_to_metastore", EXPOSED)
                .put(HIVE, "delta_lake_catalog_name", INACCESSIBLE)
                .put(HIVE, "dynamic_filtering_wait_timeout", EXPOSED)
                .put(HIVE, "dynamic_row_filtering_enabled", EXPOSED)
                .put(HIVE, "dynamic_row_filtering_selectivity_threshold", EXPOSED)
                .put(HIVE, "dynamic_row_filtering_wait_timeout", EXPOSED)
                .put(HIVE, "force_local_scheduling", EXPOSED)
                .put(HIVE, "hive_storage_format", EXPOSED)
                .put(HIVE, "hive_views_legacy_translation", EXPOSED)
                .put(HIVE, "hudi_catalog_name", INACCESSIBLE)
                .put(HIVE, "iceberg_catalog_name", INACCESSIBLE)
                .put(HIVE, "ignore_absent_partitions", EXPOSED)
                .put(HIVE, "ignore_corrupted_statistics", EXPOSED)
                .put(HIVE, "insert_existing_partitions_behavior", EXPOSED)
                .put(HIVE, "max_initial_split_size", EXPOSED)
                .put(HIVE, "max_split_size", EXPOSED)
                .put(HIVE, "metastore_stream_tables_fetch_size", EXPOSED) // Galaxy specific
                .put(HIVE, "minimum_assigned_split_weight", EXPOSED)
                .put(HIVE, "non_transactional_optimize_enabled", EXPOSED)
                .put(HIVE, "optimize_mismatched_bucket_count", EXPOSED)
                .put(HIVE, "orc_bloom_filters_enabled", EXPOSED)
                .put(HIVE, "orc_lazy_read_small_ranges", EXPOSED)
                .put(HIVE, "orc_max_buffer_size", EXPOSED)
                .put(HIVE, "orc_max_merge_distance", EXPOSED)
                .put(HIVE, "orc_max_read_block_size", EXPOSED)
                .put(HIVE, "orc_native_zstd_decompressor_enabled", EXPOSED)
                .put(HIVE, "orc_nested_lazy_enabled", EXPOSED)
                .put(HIVE, "orc_optimized_writer_max_dictionary_memory", EXPOSED)
                .put(HIVE, "orc_optimized_writer_max_stripe_rows", EXPOSED)
                .put(HIVE, "orc_optimized_writer_max_stripe_size", EXPOSED)
                .put(HIVE, "orc_optimized_writer_min_stripe_size", EXPOSED)
                .put(HIVE, "orc_optimized_writer_validate", EXPOSED)
                .put(HIVE, "orc_optimized_writer_validate_mode", EXPOSED)
                .put(HIVE, "orc_optimized_writer_validate_percentage", EXPOSED)
                .put(HIVE, "orc_stream_buffer_size", EXPOSED)
                .put(HIVE, "orc_string_statistics_limit", EXPOSED)
                .put(HIVE, "orc_tiny_stripe_threshold", EXPOSED)
                .put(HIVE, "orc_use_column_names", EXPOSED)
                .put(HIVE, "parallel_partitioned_bucketed_writes", EXPOSED)
                .put(HIVE, "parquet_ignore_statistics", EXPOSED)
                .put(HIVE, "parquet_max_read_block_row_count", EXPOSED)
                .put(HIVE, "parquet_max_read_block_size", EXPOSED)
                .put(HIVE, "parquet_native_snappy_decompressor_enabled", EXPOSED)
                .put(HIVE, "parquet_native_zstd_decompressor_enabled", EXPOSED)
                .put(HIVE, "parquet_optimized_writer_validation_percentage", EXPOSED)
                .put(HIVE, "parquet_small_file_threshold", EXPOSED)
                .put(HIVE, "parquet_use_bloom_filter", EXPOSED)
                .put(HIVE, "parquet_use_column_index", EXPOSED)
                .put(HIVE, "parquet_use_column_names", EXPOSED)
                .put(HIVE, "parquet_vectorized_decoding_enabled", EXPOSED)
                .put(HIVE, "parquet_writer_batch_size", EXPOSED)
                .put(HIVE, "parquet_writer_block_size", EXPOSED)
                .put(HIVE, "parquet_writer_page_size", EXPOSED)
                .put(HIVE, "partition_statistics_sample_size", EXPOSED)
                .put(HIVE, "projection_pushdown_enabled", EXPOSED)
                .put(HIVE, "propagate_table_scan_sorting_properties", EXPOSED)
                .put(HIVE, "query_partition_filter_required", EXPOSED)
                .put(HIVE, "query_partition_filter_required_schemas", EXPOSED)
                .put(HIVE, "rcfile_optimized_writer_validate", EXPOSED)
                .put(HIVE, "respect_table_format", EXPOSED)
                .put(HIVE, "size_based_split_weights_enabled", EXPOSED)
                .put(HIVE, "sorted_writing_enabled", EXPOSED)
                .put(HIVE, "statistics_enabled", EXPOSED)
                .put(HIVE, "target_max_file_size", EXPOSED)
                .put(HIVE, "idle_writer_min_file_size", EXPOSED)
                .put(HIVE, "timestamp_precision", EXPOSED)
                .put(HIVE, "validate_bucketing", EXPOSED)
                .put(ICEBERG, "collect_extended_statistics_on_write", EXPOSED)
                .put(ICEBERG, "compression_codec", EXPOSED)
                .put(ICEBERG, "dynamic_filtering_wait_timeout", EXPOSED)
                .put(ICEBERG, "dynamic_row_filtering_enabled", EXPOSED)
                .put(ICEBERG, "dynamic_row_filtering_selectivity_threshold", EXPOSED)
                .put(ICEBERG, "dynamic_row_filtering_wait_timeout", EXPOSED)
                .put(ICEBERG, "experimental_split_size", EXPOSED)
                .put(ICEBERG, "expire_snapshots_min_retention", EXPOSED)
                .put(ICEBERG, "extended_statistics_enabled", EXPOSED)
                .put(ICEBERG, "hive_catalog_name", INACCESSIBLE)
                .put(ICEBERG, "metastore_stream_tables_fetch_size", EXPOSED) // Galaxy specific
                .put(ICEBERG, "merge_manifests_on_write", EXPOSED)
                .put(ICEBERG, "minimum_assigned_split_weight", EXPOSED)
                .put(ICEBERG, "orc_bloom_filters_enabled", EXPOSED)
                .put(ICEBERG, "orc_lazy_read_small_ranges", EXPOSED)
                .put(ICEBERG, "orc_max_buffer_size", EXPOSED)
                .put(ICEBERG, "orc_max_merge_distance", EXPOSED)
                .put(ICEBERG, "orc_max_read_block_size", EXPOSED)
                .put(ICEBERG, "orc_native_zstd_decompressor_enabled", EXPOSED)
                .put(ICEBERG, "orc_nested_lazy_enabled", EXPOSED)
                .put(ICEBERG, "orc_stream_buffer_size", EXPOSED)
                .put(ICEBERG, "orc_string_statistics_limit", EXPOSED)
                .put(ICEBERG, "orc_tiny_stripe_threshold", EXPOSED)
                .put(ICEBERG, "orc_writer_max_dictionary_memory", EXPOSED)
                .put(ICEBERG, "orc_writer_max_stripe_rows", EXPOSED)
                .put(ICEBERG, "orc_writer_max_stripe_size", EXPOSED)
                .put(ICEBERG, "orc_writer_min_stripe_size", EXPOSED)
                .put(ICEBERG, "orc_writer_validate_mode", EXPOSED)
                .put(ICEBERG, "orc_writer_validate_percentage", EXPOSED)
                .put(ICEBERG, "parquet_max_read_block_row_count", EXPOSED)
                .put(ICEBERG, "parquet_max_read_block_size", EXPOSED)
                .put(ICEBERG, "parquet_native_snappy_decompressor_enabled", EXPOSED)
                .put(ICEBERG, "parquet_native_zstd_decompressor_enabled", EXPOSED)
                .put(ICEBERG, "parquet_small_file_threshold", EXPOSED)
                .put(ICEBERG, "parquet_use_bloom_filter", EXPOSED)
                .put(ICEBERG, "parquet_vectorized_decoding_enabled", EXPOSED)
                .put(ICEBERG, "parquet_writer_batch_size", EXPOSED)
                .put(ICEBERG, "parquet_writer_block_size", EXPOSED)
                .put(ICEBERG, "parquet_writer_page_size", EXPOSED)
                .put(ICEBERG, "projection_pushdown_enabled", EXPOSED)
                .put(ICEBERG, "query_partition_filter_required", EXPOSED)
                .put(ICEBERG, "remove_orphan_files_min_retention", EXPOSED)
                .put(ICEBERG, "sorted_writing_enabled", EXPOSED)
                .put(ICEBERG, "statistics_enabled", EXPOSED)
                .put(ICEBERG, "target_max_file_size", EXPOSED)
                .put(ICEBERG, "idle_writer_min_file_size", EXPOSED)
                .put(ICEBERG, "use_file_size_from_metadata", EXPOSED)
                .put(DELTA, "checkpoint_filtering_enabled", EXPOSED)
                .put(DELTA, "compression_codec", EXPOSED)
                .put(DELTA, "dynamic_filtering_wait_timeout", EXPOSED)
                .put(DELTA, "dynamic_row_filtering_enabled", EXPOSED)
                .put(DELTA, "dynamic_row_filtering_selectivity_threshold", EXPOSED)
                .put(DELTA, "dynamic_row_filtering_wait_timeout", EXPOSED)
                .put(DELTA, "extended_statistics_collect_on_write", EXPOSED)
                .put(DELTA, "extended_statistics_enabled", EXPOSED)
                .put(DELTA, "hive_catalog_name", INACCESSIBLE)
                .put(DELTA, "legacy_create_table_with_existing_location_enabled", EXPOSED)
                .put(DELTA, "max_initial_split_size", EXPOSED)
                .put(DELTA, "max_split_size", EXPOSED)
                .put(DELTA, "metastore_stream_tables_fetch_size", EXPOSED) // Galaxy specific
                .put(DELTA, "parquet_max_read_block_row_count", EXPOSED)
                .put(DELTA, "parquet_max_read_block_size", EXPOSED)
                .put(DELTA, "parquet_native_snappy_decompressor_enabled", EXPOSED)
                .put(DELTA, "parquet_native_zstd_decompressor_enabled", EXPOSED)
                .put(DELTA, "parquet_small_file_threshold", EXPOSED)
                .put(DELTA, "parquet_use_column_index", EXPOSED)
                .put(DELTA, "parquet_vectorized_decoding_enabled", EXPOSED)
                .put(DELTA, "parquet_writer_block_size", EXPOSED)
                .put(DELTA, "parquet_writer_page_size", EXPOSED)
                .put(DELTA, "projection_pushdown_enabled", EXPOSED)
                .put(DELTA, "query_partition_filter_required", EXPOSED)
                .put(DELTA, "statistics_enabled", EXPOSED)
                .put(DELTA, "target_max_file_size", EXPOSED)
                .put(DELTA, "idle_writer_min_file_size", EXPOSED)
                .put(DELTA, "timestamp_precision", EXPOSED)
                .put(DELTA, "vacuum_min_retention", EXPOSED)
                .put(HUDI, "columns_to_hide", EXPOSED)
                .put(HUDI, "max_outstanding_splits", EXPOSED)
                .put(HUDI, "max_splits_per_second", EXPOSED)
                .put(HUDI, "metastore_stream_tables_fetch_size", EXPOSED) // Galaxy specific
                .put(HUDI, "minimum_assigned_split_weight", EXPOSED)
                .put(HUDI, "parquet_native_snappy_decompressor_enabled", EXPOSED)
                .put(HUDI, "parquet_native_zstd_decompressor_enabled", EXPOSED)
                .put(HUDI, "parquet_small_file_threshold", EXPOSED)
                .put(HUDI, "parquet_vectorized_decoding_enabled", EXPOSED)
                .put(HUDI, "size_based_split_weights_enabled", EXPOSED)
                .put(HUDI, "split_generator_parallelism", EXPOSED)
                .put(HUDI, "standard_split_weight_size", EXPOSED)
                .put(HUDI, "use_parquet_column_names", EXPOSED)
                .buildOrThrow();
    }

    public static Table<TableType, String, FeatureExposure> procedureExposureDecisions()
    {
        return ImmutableTable.<TableType, String, FeatureExposure>builder()
                // sorted by procedure name, then by connector in (Hive, Iceberg, Delta, Hudi) order
                .put(HIVE, "create_empty_partition", EXPOSED)
                .put(HIVE, "drop_stats", EXPOSED) // TODO similar to drop_extended_stats exposed by Delta
                .put(DELTA, "drop_extended_stats", EXPOSED) // TODO similar to drop_stats exposed by hive and similar to DROP_EXTENDED_STATS *table procedure* exposed by Iceberg
                .put(HIVE, "flush_metadata_cache", INACCESSIBLE) // see ObjectStoreFlushMetadataCache
                .put(DELTA, "flush_metadata_cache", INACCESSIBLE) // see ObjectStoreFlushMetadataCache
                .put(ICEBERG, "migrate", INACCESSIBLE) // supported via ALTER TABLE SET PROPERTIES syntax
                .put(HIVE, "register_partition", EXPOSED)
                .put(DELTA, "register_table", INACCESSIBLE) // see ObjectStoreRegisterTableProcedure
                .put(ICEBERG, "register_table", INACCESSIBLE) // see ObjectStoreRegisterTableProcedure
                .put(ICEBERG, "rollback_to_snapshot", EXPOSED)
                .put(HIVE, "sync_partition_metadata", EXPOSED)
                .put(HIVE, "unregister_partition", EXPOSED)
                .put(DELTA, "unregister_table", INACCESSIBLE) // see ObjectStoreUnregisterTableProcedure
                .put(ICEBERG, "unregister_table", INACCESSIBLE) // see ObjectStoreUnregisterTableProcedure
                .put(DELTA, "vacuum", EXPOSED)
                .buildOrThrow();
    }

    public static Table<TableType, String, FeatureExposure> tableProcedureExposureDecisions()
    {
        return ImmutableTable.<TableType, String, FeatureExposure>builder()
                // sorted by procedure name, then by connector in (Hive, Iceberg, Delta, Hudi) order
                .put(ICEBERG, "DROP_EXTENDED_STATS", EXPOSED) // TODO similar to drop_extended_stats *procedure* exposed by Delta
                .put(ICEBERG, "EXPIRE_SNAPSHOTS", EXPOSED)
                .put(ICEBERG, "REMOVE_ORPHAN_FILES", EXPOSED)
                .put(HIVE, "OPTIMIZE", INACCESSIBLE)
                .put(ICEBERG, "OPTIMIZE", EXPOSED)
                .put(DELTA, "OPTIMIZE", EXPOSED)
                .buildOrThrow();
    }
}
