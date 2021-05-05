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
package io.trino.plugin.hive.metastore.galaxy;

import io.starburst.stargate.metastore.client.BasicStatistics;
import io.starburst.stargate.metastore.client.BucketProperty;
import io.starburst.stargate.metastore.client.ColumnStatistics;
import io.starburst.stargate.metastore.client.PartitionName;
import io.starburst.stargate.metastore.client.Statistics;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.metastore.BooleanStatistics;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.DateStatistics;
import io.trino.plugin.hive.metastore.DecimalStatistics;
import io.trino.plugin.hive.metastore.DoubleStatistics;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.IntegerStatistics;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.SortingColumn.Order;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;

final class GalaxyMetastoreUtils
{
    static final String PUBLIC_ROLE_NAME = "public";

    private GalaxyMetastoreUtils() {}

    public static Database fromGalaxyDatabase(io.starburst.stargate.metastore.client.Database database)
    {
        return new Database(
                database.databaseName(),
                database.location(),
                Optional.empty(),
                Optional.empty(),
                database.comment(),
                database.parameters());
    }

    public static io.starburst.stargate.metastore.client.Database toGalaxyDatabase(Database database)
    {
        return new io.starburst.stargate.metastore.client.Database(
                database.getDatabaseName(),
                database.getLocation(),
                database.getComment(),
                database.getParameters());
    }

    public static Table fromGalaxyTable(io.starburst.stargate.metastore.client.Table table)
    {
        return new Table(
                table.databaseName(),
                table.tableName(),
                Optional.empty(),
                table.tableType(),
                fromGalaxyStorage(table.storage()),
                fromGalaxyColumns(table.dataColumns()),
                fromGalaxyColumns(table.partitionColumns()),
                table.parameters(),
                table.viewOriginalText(),
                table.viewExpandedText(),
                OptionalLong.empty());
    }

    public static io.starburst.stargate.metastore.client.Table toGalaxyTable(Table table)
    {
        return new io.starburst.stargate.metastore.client.Table(
                table.getDatabaseName(),
                table.getTableName(),
                table.getTableType(),
                toGalaxyStorage(table.getStorage()),
                toGalaxyColumns(table.getDataColumns()),
                toGalaxyColumns(table.getPartitionColumns()),
                table.getParameters(),
                table.getViewOriginalText(),
                table.getViewExpandedText());
    }

    public static Partition fromGalaxyPartition(io.starburst.stargate.metastore.client.Partition partition)
    {
        return new Partition(
                partition.databaseName(),
                partition.tableName(),
                partition.partitionName().partitionValues(),
                fromGalaxyStorage(partition.storage()),
                fromGalaxyColumns(partition.dataColumns()),
                partition.parameters());
    }

    public static io.starburst.stargate.metastore.client.Partition toGalaxyPartition(Partition partition)
    {
        return new io.starburst.stargate.metastore.client.Partition(
                partition.getDatabaseName(),
                partition.getTableName(),
                new PartitionName(partition.getValues()),
                toGalaxyStorage(partition.getStorage()),
                toGalaxyColumns(partition.getColumns()),
                partition.getParameters());
    }

    public static io.starburst.stargate.metastore.client.PartitionWithStatistics toGalaxyPartitionWithStatistics(PartitionWithStatistics partition)
    {
        return new io.starburst.stargate.metastore.client.PartitionWithStatistics(
                toGalaxyPartition(partition.getPartition()),
                toGalaxyStatistics(partition.getStatistics()));
    }

    public static Column fromGalaxyColumn(io.starburst.stargate.metastore.client.Column column)
    {
        return new Column(column.name(), HiveType.valueOf(column.type()), column.comment());
    }

    private static List<Column> fromGalaxyColumns(List<io.starburst.stargate.metastore.client.Column> columns)
    {
        return columns.stream()
                .map(GalaxyMetastoreUtils::fromGalaxyColumn)
                .collect(toImmutableList());
    }

    public static io.starburst.stargate.metastore.client.Column toGalaxyColumn(Column column)
    {
        return new io.starburst.stargate.metastore.client.Column(column.getName(), column.getType().toString(), column.getComment());
    }

    private static List<io.starburst.stargate.metastore.client.Column> toGalaxyColumns(List<Column> columns)
    {
        return columns.stream()
                .map(GalaxyMetastoreUtils::toGalaxyColumn)
                .collect(toImmutableList());
    }

    public static SortingColumn fromGalaxySortingColumn(io.starburst.stargate.metastore.client.SortingColumn column)
    {
        return new SortingColumn(column.columnName(), fromGalaxyOrder(column.order()));
    }

    public static io.starburst.stargate.metastore.client.SortingColumn toGalaxySortingColumn(SortingColumn column)
    {
        return new io.starburst.stargate.metastore.client.SortingColumn(column.getColumnName(), toGalaxyOrder(column.getOrder()));
    }

    public static Order fromGalaxyOrder(io.starburst.stargate.metastore.client.SortingColumn.Order order)
    {
        switch (order) {
            case ASCENDING:
                return Order.ASCENDING;
            case DESCENDING:
                return Order.DESCENDING;
            default:
                throw new IllegalArgumentException("Unknown order " + order);
        }
    }

    public static io.starburst.stargate.metastore.client.SortingColumn.Order toGalaxyOrder(Order order)
    {
        switch (order) {
            case ASCENDING:
                return io.starburst.stargate.metastore.client.SortingColumn.Order.ASCENDING;
            case DESCENDING:
                return io.starburst.stargate.metastore.client.SortingColumn.Order.DESCENDING;
            default:
                throw new IllegalArgumentException("Unknown order " + order);
        }
    }

    public static Storage fromGalaxyStorage(io.starburst.stargate.metastore.client.Storage storage)
    {
        Optional<HiveBucketProperty> bucketProperty = storage.bucketProperty()
                .map(GalaxyMetastoreUtils::fromGalaxyBucketProperty);
        return new Storage(
                fromGalaxyStorageFormat(storage.storageFormat()),
                Optional.of(storage.location()),
                bucketProperty,
                storage.skewed(),
                storage.serdeParameters());
    }

    public static io.starburst.stargate.metastore.client.Storage toGalaxyStorage(Storage storage)
    {
        Optional<BucketProperty> bucketProperty = storage.getBucketProperty()
                .map(GalaxyMetastoreUtils::toGalaxyBucketProperty);
        return new io.starburst.stargate.metastore.client.Storage(
                toGalaxyStorageFormat(storage.getStorageFormat()),
                storage.getLocation(),
                bucketProperty,
                storage.isSkewed(),
                storage.getSerdeParameters());
    }

    public static HiveBucketProperty fromGalaxyBucketProperty(BucketProperty bucketProperty)
    {
        return new HiveBucketProperty(
                bucketProperty.bucketedBy(),
                BucketingVersion.BUCKETING_V1,
                bucketProperty.bucketCount(),
                bucketProperty.sortedBy().stream()
                        .map(GalaxyMetastoreUtils::fromGalaxySortingColumn)
                        .collect(toImmutableList()));
    }

    public static BucketProperty toGalaxyBucketProperty(HiveBucketProperty bucketProperty)
    {
        return new BucketProperty(
                bucketProperty.getBucketedBy(),
                bucketProperty.getBucketCount(),
                bucketProperty.getSortedBy().stream()
                        .map(GalaxyMetastoreUtils::toGalaxySortingColumn)
                        .collect(toImmutableList()));
    }

    public static StorageFormat fromGalaxyStorageFormat(io.starburst.stargate.metastore.client.StorageFormat storageFormat)
    {
        return StorageFormat.createNullable(storageFormat.serDe(), storageFormat.inputFormat(), storageFormat.outputFormat());
    }

    public static io.starburst.stargate.metastore.client.StorageFormat toGalaxyStorageFormat(StorageFormat storageFormat)
    {
        return new io.starburst.stargate.metastore.client.StorageFormat(
                storageFormat.getSerDeNullable(),
                storageFormat.getInputFormatNullable(),
                storageFormat.getOutputFormatNullable());
    }

    public static PartitionStatistics fromGalaxyStatistics(Statistics statistics)
    {
        return new PartitionStatistics(
                fromGalaxyBasicStatistics(statistics.basicStatistics()),
                statistics.columnStatistics().entrySet().stream()
                        .map(entry -> immutableEntry(entry.getKey(), fromGalaxyColumnStatistics(entry.getValue())))
                        .collect(toImmutableMap(Entry::getKey, Entry::getValue)));
    }

    public static Statistics toGalaxyStatistics(PartitionStatistics statistics)
    {
        return new Statistics(
                toGalaxyBasicStatistics(statistics.getBasicStatistics()),
                statistics.getColumnStatistics().entrySet().stream()
                        .map(entry -> immutableEntry(entry.getKey(), toGalaxyColumnStatistics(entry.getValue())))
                        .collect(toImmutableMap(Entry::getKey, Entry::getValue)));
    }

    public static HiveBasicStatistics fromGalaxyBasicStatistics(BasicStatistics basicStatistics)
    {
        return new HiveBasicStatistics(
                basicStatistics.fileCount(),
                basicStatistics.rowCount(),
                basicStatistics.inMemoryDataSizeInBytes(),
                basicStatistics.onDiskDataSizeInBytes());
    }

    private static BasicStatistics toGalaxyBasicStatistics(HiveBasicStatistics statistics)
    {
        return new BasicStatistics(
                statistics.getFileCount(),
                statistics.getRowCount(),
                statistics.getInMemoryDataSizeInBytes(),
                statistics.getOnDiskDataSizeInBytes());
    }

    public static HiveColumnStatistics fromGalaxyColumnStatistics(ColumnStatistics columnStatistics)
    {
        return new HiveColumnStatistics(
                columnStatistics.integerStatistics().map(GalaxyMetastoreUtils::fromGalaxyIntegerStatistics),
                columnStatistics.doubleStatistics().map(GalaxyMetastoreUtils::fromGalaxyDoubleStatistics),
                columnStatistics.decimalStatistics().map(GalaxyMetastoreUtils::fromGalaxyDecimalStatistics),
                columnStatistics.dateStatistics().map(GalaxyMetastoreUtils::fromGalaxyDateStatistics),
                columnStatistics.booleanStatistics().map(GalaxyMetastoreUtils::fromGalaxyBooleanStatistics),
                columnStatistics.maxValueSizeInBytes(),
                columnStatistics.totalSizeInBytes(),
                columnStatistics.nullsCount(),
                columnStatistics.distinctValuesCount());
    }

    public static ColumnStatistics toGalaxyColumnStatistics(HiveColumnStatistics columnStatistics)
    {
        return new ColumnStatistics(
                columnStatistics.getIntegerStatistics().map(GalaxyMetastoreUtils::toGalaxyIntegerStatistics),
                columnStatistics.getDoubleStatistics().map(GalaxyMetastoreUtils::toGalaxyDoubleStatistics),
                columnStatistics.getDecimalStatistics().map(GalaxyMetastoreUtils::toGalaxyDecimalStatistics),
                columnStatistics.getDateStatistics().map(GalaxyMetastoreUtils::toGalaxyDateStatistics),
                columnStatistics.getBooleanStatistics().map(GalaxyMetastoreUtils::toGalaxyBooleanStatistics),
                columnStatistics.getMaxValueSizeInBytes(),
                columnStatistics.getTotalSizeInBytes(),
                columnStatistics.getNullsCount(),
                columnStatistics.getDistinctValuesCount());
    }

    public static IntegerStatistics fromGalaxyIntegerStatistics(ColumnStatistics.IntegerStatistics integerStatistics)
    {
        return new IntegerStatistics(integerStatistics.min(), integerStatistics.max());
    }

    public static ColumnStatistics.IntegerStatistics toGalaxyIntegerStatistics(IntegerStatistics integerStatistics)
    {
        return new ColumnStatistics.IntegerStatistics(integerStatistics.getMin(), integerStatistics.getMax());
    }

    public static DoubleStatistics fromGalaxyDoubleStatistics(ColumnStatistics.DoubleStatistics doubleStatistics)
    {
        return new DoubleStatistics(doubleStatistics.min(), doubleStatistics.max());
    }

    public static ColumnStatistics.DoubleStatistics toGalaxyDoubleStatistics(DoubleStatistics doubleStatistics)
    {
        return new ColumnStatistics.DoubleStatistics(doubleStatistics.getMin(), doubleStatistics.getMax());
    }

    public static DecimalStatistics fromGalaxyDecimalStatistics(ColumnStatistics.DecimalStatistics decimalStatistics)
    {
        return new DecimalStatistics(decimalStatistics.min(), decimalStatistics.max());
    }

    public static ColumnStatistics.DecimalStatistics toGalaxyDecimalStatistics(DecimalStatistics decimalStatistics)
    {
        return new ColumnStatistics.DecimalStatistics(decimalStatistics.getMin(), decimalStatistics.getMax());
    }

    public static DateStatistics fromGalaxyDateStatistics(ColumnStatistics.DateStatistics dateStatistics)
    {
        return new DateStatistics(dateStatistics.min(), dateStatistics.max());
    }

    public static ColumnStatistics.DateStatistics toGalaxyDateStatistics(DateStatistics dateStatistics)
    {
        return new ColumnStatistics.DateStatistics(dateStatistics.getMin(), dateStatistics.getMax());
    }

    public static BooleanStatistics fromGalaxyBooleanStatistics(ColumnStatistics.BooleanStatistics integerStatistics)
    {
        return new BooleanStatistics(integerStatistics.trueCount(), integerStatistics.falseCount());
    }

    public static ColumnStatistics.BooleanStatistics toGalaxyBooleanStatistics(BooleanStatistics integerStatistics)
    {
        return new ColumnStatistics.BooleanStatistics(integerStatistics.getTrueCount(), integerStatistics.getFalseCount());
    }

    public static List<PartitionName> toPartitionNames(Table table, List<Partition> partitions)
    {
        return partitions.stream()
                .map(Partition::getValues)
                .map(PartitionName::new)
                .collect(toImmutableList());
    }
}
