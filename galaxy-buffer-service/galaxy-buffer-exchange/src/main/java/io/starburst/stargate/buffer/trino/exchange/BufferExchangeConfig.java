/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.net.URI;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.trino.exchange.PartitionNodeMappingMode.PINNING_MULTI;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BufferExchangeConfig
{
    private URI discoveryServiceUri;
    private DataSize sinkBlockedMemoryLowWaterMark = DataSize.of(128, MEGABYTE);
    private DataSize sinkBlockedMemoryHighWaterMark = DataSize.of(256, MEGABYTE);
    private DataSize sourceBlockedMemoryLowWaterMark = DataSize.of(32, MEGABYTE);
    private DataSize sourceBlockedMemoryHighWaterMark = DataSize.of(64, MEGABYTE);
    private int sourceParallelism = 16;
    private int sourceHandleTargetChunksCount = 64;
    private DataSize sourceHandleTargetDataSize = DataSize.of(256, MEGABYTE);
    private Duration sinkWriterMaxWait = succinctDuration(1, SECONDS);
    private int sinkMinWrittenPagesCount = 32;
    private DataSize sinkMinWrittenPagesSize = DataSize.of(1, MEGABYTE);
    private int sinkTargetWrittenPagesCount = 512;
    private DataSize sinkTargetWrittenPagesSize = DataSize.of(16, MEGABYTE);
    private int sinkTargetWrittenPartitionsCount = 16;
    private Duration sinkMinTimeBetweenWriterScaleUps = succinctDuration(5.0, SECONDS);
    private double sinkMaxWritersScaleUpGrowthFactor = 2.0;
    private PartitionNodeMappingMode partitionNodeMappingMode = PINNING_MULTI;
    private int minBaseBufferNodesPerPartition = 2;
    private int maxBaseBufferNodesPerPartition = 32;
    private double bonusBufferNodesPerPartitionMultiplier = 4.0;
    private int minTotalBufferNodesPerPartition = 16;
    private int maxTotalBufferNodesPerPartition = 64;
    private Duration maxWaitActiveBufferNodes = succinctDuration(5, MINUTES);

    private int dataClientMaxRetries = 5;
    private Duration dataClientRetryBackoffInitial = succinctDuration(2.0, SECONDS);
    private Duration dataClientRetryBackoffMax = succinctDuration(60.0, SECONDS);
    private double dataClientRetryBackoffFactor = 2.0;
    private double dataClientRetryBackoffJitter = 0.5;
    private int dataClientCircuitBreakerFailureThreshold = 10;
    private int dataClientCircuitBreakerSuccessThreshold = 5;
    private Duration dataClientCircuitBreakerDelay = succinctDuration(30.0, SECONDS);

    private int dataClientAddDataPagesMaxRetries = 5;
    private Duration dataClientAddDataPagesRetryBackoffInitial = succinctDuration(16.0, SECONDS);
    private Duration dataClientAddDataPagesRetryBackoffMax = succinctDuration(120.0, SECONDS);
    private double dataClientAddDataPagesRetryBackoffFactor = 2.0;
    private double dataClientAddDataPagesRetryBackoffJitter = 0.5;
    private int dataClientAddDataPagesCircuitBreakerFailureThreshold = 10;
    private int dataClientAddDataPagesCircuitBreakerSuccessThreshold = 5;
    private Duration dataClientAddDataPagesCircuitBreakerDelay = succinctDuration(60.0, SECONDS);

    @NotNull
    public URI getDiscoveryServiceUri()
    {
        return discoveryServiceUri;
    }

    @Config("exchange.buffer-discovery.uri")
    @ConfigDescription("Buffer discovery service URI")
    public BufferExchangeConfig setDiscoveryServiceUri(URI discoveryServiceUri)
    {
        this.discoveryServiceUri = discoveryServiceUri;
        return this;
    }

    public DataSize getSinkBlockedMemoryLowWaterMark()
    {
        return sinkBlockedMemoryLowWaterMark;
    }

    @Config("exchange.sink-blocked-memory-low")
    @ConfigDescription("Sink blocked memory low water mark")
    public BufferExchangeConfig setSinkBlockedMemoryLowWaterMark(DataSize sinkBlockedMemoryLowWaterMark)
    {
        this.sinkBlockedMemoryLowWaterMark = sinkBlockedMemoryLowWaterMark;
        return this;
    }

    public DataSize getSinkBlockedMemoryHighWaterMark()
    {
        return sinkBlockedMemoryHighWaterMark;
    }

    @Config("exchange.sink-blocked-memory-high")
    @ConfigDescription("Sink blocked memory high water mark")
    public BufferExchangeConfig setSinkBlockedMemoryHighWaterMark(DataSize sinkBlockedMemoryHighWaterMark)
    {
        this.sinkBlockedMemoryHighWaterMark = sinkBlockedMemoryHighWaterMark;
        return this;
    }

    public DataSize getSourceBlockedMemoryLowWaterMark()
    {
        return sourceBlockedMemoryLowWaterMark;
    }

    @Config("exchange.source-blocked-memory-low")
    @ConfigDescription("Source blocked memory low water mark")
    public BufferExchangeConfig setSourceBlockedMemoryLowWaterMark(DataSize sourceBlockedMemoryLowWaterMark)
    {
        this.sourceBlockedMemoryLowWaterMark = sourceBlockedMemoryLowWaterMark;
        return this;
    }

    public DataSize getSourceBlockedMemoryHighWaterMark()
    {
        return sourceBlockedMemoryHighWaterMark;
    }

    @Config("exchange.source-blocked-memory-high")
    @ConfigDescription("Source blocked memory high water mark")
    public BufferExchangeConfig setSourceBlockedMemoryHighWaterMark(DataSize sourceBlockedMemoryHighWaterMark)
    {
        this.sourceBlockedMemoryHighWaterMark = sourceBlockedMemoryHighWaterMark;
        return this;
    }

    public int getSourceParallelism()
    {
        return sourceParallelism;
    }

    @Config("exchange.source-parallelism")
    @ConfigDescription("Source data reading request parallelism")
    public BufferExchangeConfig setSourceParallelism(int sourceParallelism)
    {
        this.sourceParallelism = sourceParallelism;
        return this;
    }

    public int getSourceHandleTargetChunksCount()
    {
        return sourceHandleTargetChunksCount;
    }

    @Config("exchange.source-handle-target-chunks-count")
    @ConfigDescription("Target number of chunks referenced by a single source handle")
    public BufferExchangeConfig setSourceHandleTargetChunksCount(int sourceHandleTargetChunksCount)
    {
        this.sourceHandleTargetChunksCount = sourceHandleTargetChunksCount;
        return this;
    }

    @NotNull
    public DataSize getSourceHandleTargetDataSize()
    {
        return sourceHandleTargetDataSize;
    }

    @Config("exchange.source-handle-target-data-size")
    @ConfigDescription("Target size of the data referenced by a single source handle")
    public BufferExchangeConfig setSourceHandleTargetDataSize(DataSize sourceHandleTargetDataSize)
    {
        this.sourceHandleTargetDataSize = sourceHandleTargetDataSize;
        return this;
    }

    public Duration getSinkWriterMaxWait()
    {
        return sinkWriterMaxWait;
    }

    @Config("exchange.sink-writer-max-wait")
    @ConfigDescription("Max wait interval before we send data pages to buffer nodes from the sink data pool")
    public BufferExchangeConfig setSinkWriterMaxWait(Duration sinkWriterMaxWait)
    {
        this.sinkWriterMaxWait = sinkWriterMaxWait;
        return this;
    }

    public int getSinkMinWrittenPagesCount()
    {
        return sinkMinWrittenPagesCount;
    }

    @Config("exchange.sink-min-written-pages-count")
    @ConfigDescription("Min pages count when we send data pages to buffer nodes within the max wait")
    public BufferExchangeConfig setSinkMinWrittenPagesCount(int sinkMinWrittenPagesCount)
    {
        this.sinkMinWrittenPagesCount = sinkMinWrittenPagesCount;
        return this;
    }

    public DataSize getSinkMinWrittenPagesSize()
    {
        return sinkMinWrittenPagesSize;
    }

    @Config("exchange.sink-min-written-pages-size")
    @ConfigDescription("Min data size when we send data pages to buffer nodes within the max wait")
    public BufferExchangeConfig setSinkMinWrittenPagesSize(DataSize sinkMinWrittenPagesSize)
    {
        this.sinkMinWrittenPagesSize = sinkMinWrittenPagesSize;
        return this;
    }

    public int getSinkTargetWrittenPagesCount()
    {
        return sinkTargetWrittenPagesCount;
    }

    @Config("exchange.sink-target-written-pages-count")
    @ConfigDescription("Target number of pages to be sent in single HTTP request from sink to buffer service")
    public BufferExchangeConfig setSinkTargetWrittenPagesCount(int sinkTargetWrittenPagesCount)
    {
        this.sinkTargetWrittenPagesCount = sinkTargetWrittenPagesCount;
        return this;
    }

    public DataSize getSinkTargetWrittenPagesSize()
    {
        return sinkTargetWrittenPagesSize;
    }

    @Config("exchange.sink-target-written-pages-size")
    @ConfigDescription("Target size of data to be sent in single HTTP request from sink to buffer service")
    public BufferExchangeConfig setSinkTargetWrittenPagesSize(DataSize sinkTargetWrittenPagesSize)
    {
        this.sinkTargetWrittenPagesSize = sinkTargetWrittenPagesSize;
        return this;
    }

    public int getSinkTargetWrittenPartitionsCount()
    {
        return sinkTargetWrittenPartitionsCount;
    }

    @Config("exchange.sink-target-written-partitions-count")
    @ConfigDescription("Target number of partitions written to in a single HTTP request from sink to buffer service")
    public BufferExchangeConfig setSinkTargetWrittenPartitionsCount(int sinkTargetWrittenPartitionsCount)
    {
        this.sinkTargetWrittenPartitionsCount = sinkTargetWrittenPartitionsCount;
        return this;
    }

    public Duration getSinkMinTimeBetweenWriterScaleUps()
    {
        return sinkMinTimeBetweenWriterScaleUps;
    }

    @Config("exchange.sink-min-time-between-writer-scale-ups")
    @ConfigDescription("How much time must pass since we last scaled up writers for a task, before next scale up")
    public BufferExchangeConfig setSinkMinTimeBetweenWriterScaleUps(Duration sinkMinTimeBetweenWriterScaleUps)
    {
        this.sinkMinTimeBetweenWriterScaleUps = sinkMinTimeBetweenWriterScaleUps;
        return this;
    }

    @Min(1)
    public double getSinkMaxWritersScaleUpGrowthFactor()
    {
        return sinkMaxWritersScaleUpGrowthFactor;
    }

    @Config("exchange.sink-max-writers-scale-up-growth-factor")
    @ConfigDescription("By how much can we multiply number of workers for a partition in each scale up round")
    public BufferExchangeConfig setSinkMaxWritersScaleUpGrowthFactor(double sinkMaxWritersScaleUpGrowthFactor)
    {
        this.sinkMaxWritersScaleUpGrowthFactor = sinkMaxWritersScaleUpGrowthFactor;
        return this;
    }

    @Config("exchange.partition-node-mapping-mode")
    @ConfigDescription("How are the output partitions of Trino tasks mapped to buffer service nodes")
    public BufferExchangeConfig setPartitionNodeMappingMode(PartitionNodeMappingMode partitionNodeMappingMode)
    {
        this.partitionNodeMappingMode = partitionNodeMappingMode;
        return this;
    }

    public PartitionNodeMappingMode getPartitionNodeMappingMode()
    {
        return partitionNodeMappingMode;
    }

    @Min(1)
    public int getMinBaseBufferNodesPerPartition()
    {
        return minBaseBufferNodesPerPartition;
    }

    @Config("exchange.min-base-buffer-nodes-per-partition")
    @LegacyConfig("exchange.min-buffer-nodes-per-partition")
    public BufferExchangeConfig setMinBaseBufferNodesPerPartition(int minBaseBufferNodesPerPartition)
    {
        this.minBaseBufferNodesPerPartition = minBaseBufferNodesPerPartition;
        return this;
    }

    @Min(1)
    public int getMaxBaseBufferNodesPerPartition()
    {
        return maxBaseBufferNodesPerPartition;
    }

    @Config("exchange.max-base-buffer-nodes-per-partition")
    @LegacyConfig("exchange.max-buffer-nodes-per-partition")
    public BufferExchangeConfig setMaxBaseBufferNodesPerPartition(int maxBaseBufferNodesPerPartition)
    {
        this.maxBaseBufferNodesPerPartition = maxBaseBufferNodesPerPartition;
        return this;
    }

    public double getBonusBufferNodesPerPartitionMultiplier()
    {
        return bonusBufferNodesPerPartitionMultiplier;
    }

    @Config("exchange.bonus-buffer-nodes-per-partition-multiplier")
    @ConfigDescription("Multiplier we use to compute number of bonus nodes from number of base nodes assigned to partition")
    public BufferExchangeConfig setBonusBufferNodesPerPartitionMultiplier(double bonusBufferNodesPerPartitionMultiplier)
    {
        this.bonusBufferNodesPerPartitionMultiplier = bonusBufferNodesPerPartitionMultiplier;
        return this;
    }

    public int getMinTotalBufferNodesPerPartition()
    {
        return minTotalBufferNodesPerPartition;
    }

    @Config("exchange.min-total-buffer-nodes-per-partition")
    public BufferExchangeConfig setMinTotalBufferNodesPerPartition(int minTotalBufferNodesPerPartition)
    {
        this.minTotalBufferNodesPerPartition = minTotalBufferNodesPerPartition;
        return this;
    }

    public int getMaxTotalBufferNodesPerPartition()
    {
        return maxTotalBufferNodesPerPartition;
    }

    @Config("exchange.max-total-buffer-nodes-per-partition")
    public BufferExchangeConfig setMaxTotalBufferNodesPerPartition(int maxTotalBufferNodesPerPartition)
    {
        this.maxTotalBufferNodesPerPartition = maxTotalBufferNodesPerPartition;
        return this;
    }

    public Duration getMaxWaitActiveBufferNodes()
    {
        return maxWaitActiveBufferNodes;
    }

    @Config("exchange.max-wait-active-buffer-nodes")
    public BufferExchangeConfig setMaxWaitActiveBufferNodes(Duration maxWaitActiveBufferNodes)
    {
        this.maxWaitActiveBufferNodes = maxWaitActiveBufferNodes;
        return this;
    }

    public int getDataClientMaxRetries()
    {
        return dataClientMaxRetries;
    }

    @Config("exchange.buffer-data.max-retries")
    public BufferExchangeConfig setDataClientMaxRetries(int dataClientMaxRetries)
    {
        this.dataClientMaxRetries = dataClientMaxRetries;
        return this;
    }

    public Duration getDataClientRetryBackoffInitial()
    {
        return dataClientRetryBackoffInitial;
    }

    @Config("exchange.buffer-data.retry-backoff-initial")
    public BufferExchangeConfig setDataClientRetryBackoffInitial(Duration dataClientRetryBackoffInitial)
    {
        this.dataClientRetryBackoffInitial = dataClientRetryBackoffInitial;
        return this;
    }

    public Duration getDataClientRetryBackoffMax()
    {
        return dataClientRetryBackoffMax;
    }

    @Config("exchange.buffer-data.retry-backoff-max")
    public BufferExchangeConfig setDataClientRetryBackoffMax(Duration dataClientRetryBackoffMax)
    {
        this.dataClientRetryBackoffMax = dataClientRetryBackoffMax;
        return this;
    }

    public double getDataClientRetryBackoffFactor()
    {
        return dataClientRetryBackoffFactor;
    }

    @Config("exchange.buffer-data.retry-backoff-factor")
    public BufferExchangeConfig setDataClientRetryBackoffFactor(double dataClientRetryBackoffFactor)
    {
        this.dataClientRetryBackoffFactor = dataClientRetryBackoffFactor;
        return this;
    }

    public double getDataClientRetryBackoffJitter()
    {
        return dataClientRetryBackoffJitter;
    }

    @Config("exchange.buffer-data.retry-backoff-jitter")
    public BufferExchangeConfig setDataClientRetryBackoffJitter(double dataClientRetryBackoffJitter)
    {
        this.dataClientRetryBackoffJitter = dataClientRetryBackoffJitter;
        return this;
    }

    public int getDataClientCircuitBreakerFailureThreshold()
    {
        return dataClientCircuitBreakerFailureThreshold;
    }

    @Config("exchange.buffer-data.circuit-breaker-failure-threshold")
    public BufferExchangeConfig setDataClientCircuitBreakerFailureThreshold(int dataClientCircuitBreakerFailureThreshold)
    {
        this.dataClientCircuitBreakerFailureThreshold = dataClientCircuitBreakerFailureThreshold;
        return this;
    }

    public int getDataClientCircuitBreakerSuccessThreshold()
    {
        return dataClientCircuitBreakerSuccessThreshold;
    }

    @Config("exchange.buffer-data.circuit-breaker-success-threshold")
    public BufferExchangeConfig setDataClientCircuitBreakerSuccessThreshold(int dataClientCircuitBreakerSuccessThreshold)
    {
        this.dataClientCircuitBreakerSuccessThreshold = dataClientCircuitBreakerSuccessThreshold;
        return this;
    }

    public Duration getDataClientCircuitBreakerDelay()
    {
        return dataClientCircuitBreakerDelay;
    }

    @Config("exchange.buffer-data.circuit-breaker-delay")
    public BufferExchangeConfig setDataClientCircuitBreakerDelay(Duration dataClientCircuitBreakerDelay)
    {
        this.dataClientCircuitBreakerDelay = dataClientCircuitBreakerDelay;
        return this;
    }

    public int getDataClientAddDataPagesMaxRetries()
    {
        return dataClientAddDataPagesMaxRetries;
    }

    @Config("exchange.buffer-data.add-data-pages-max-retries")
    public BufferExchangeConfig setDataClientAddDataPagesMaxRetries(int dataClientAddDataPagesMaxRetries)
    {
        this.dataClientAddDataPagesMaxRetries = dataClientAddDataPagesMaxRetries;
        return this;
    }

    public Duration getDataClientAddDataPagesRetryBackoffInitial()
    {
        return dataClientAddDataPagesRetryBackoffInitial;
    }

    @Config("exchange.buffer-data.add-data-pages-retry-backoff-initial")
    public BufferExchangeConfig setDataClientAddDataPagesRetryBackoffInitial(Duration dataClientAddDataPagesRetryBackoffInitial)
    {
        this.dataClientAddDataPagesRetryBackoffInitial = dataClientAddDataPagesRetryBackoffInitial;
        return this;
    }

    public Duration getDataClientAddDataPagesRetryBackoffMax()
    {
        return dataClientAddDataPagesRetryBackoffMax;
    }

    @Config("exchange.buffer-data.add-data-pages-retry-backoff-max")
    public BufferExchangeConfig setDataClientAddDataPagesRetryBackoffMax(Duration dataClientAddDataPagesRetryBackoffMax)
    {
        this.dataClientAddDataPagesRetryBackoffMax = dataClientAddDataPagesRetryBackoffMax;
        return this;
    }

    public double getDataClientAddDataPagesRetryBackoffFactor()
    {
        return dataClientAddDataPagesRetryBackoffFactor;
    }

    @Config("exchange.buffer-data.add-data-pages-retry-backoff-factor")
    public BufferExchangeConfig setDataClientAddDataPagesRetryBackoffFactor(double dataClientAddDataPagesRetryBackoffFactor)
    {
        this.dataClientAddDataPagesRetryBackoffFactor = dataClientAddDataPagesRetryBackoffFactor;
        return this;
    }

    public double getDataClientAddDataPagesRetryBackoffJitter()
    {
        return dataClientAddDataPagesRetryBackoffJitter;
    }

    @Config("exchange.buffer-data.add-data-pages-retry-backoff-jitter")
    public BufferExchangeConfig setDataClientAddDataPagesRetryBackoffJitter(double dataClientAddDataPagesRetryBackoffJitter)
    {
        this.dataClientAddDataPagesRetryBackoffJitter = dataClientAddDataPagesRetryBackoffJitter;
        return this;
    }

    public int getDataClientAddDataPagesCircuitBreakerFailureThreshold()
    {
        return dataClientAddDataPagesCircuitBreakerFailureThreshold;
    }

    @Config("exchange.buffer-data.add-data-pages-circuit-breaker-failure-threshold")
    public BufferExchangeConfig setDataClientAddDataPagesCircuitBreakerFailureThreshold(int dataClientAddDataPagesCircuitBreakerFailureThreshold)
    {
        this.dataClientAddDataPagesCircuitBreakerFailureThreshold = dataClientAddDataPagesCircuitBreakerFailureThreshold;
        return this;
    }

    public int getDataClientAddDataPagesCircuitBreakerSuccessThreshold()
    {
        return dataClientAddDataPagesCircuitBreakerSuccessThreshold;
    }

    @Config("exchange.buffer-data.add-data-pages-circuit-breaker-success-threshold")
    public BufferExchangeConfig setDataClientAddDataPagesCircuitBreakerSuccessThreshold(int dataClientAddDataPagesCircuitBreakerSuccessThreshold)
    {
        this.dataClientAddDataPagesCircuitBreakerSuccessThreshold = dataClientAddDataPagesCircuitBreakerSuccessThreshold;
        return this;
    }

    public Duration getDataClientAddDataPagesCircuitBreakerDelay()
    {
        return dataClientAddDataPagesCircuitBreakerDelay;
    }

    @Config("exchange.buffer-data.add-data-pages-circuit-breaker-delay")
    public BufferExchangeConfig setDataClientAddDataPagesCircuitBreakerDelay(Duration dataClientAddDataPagesCircuitBreakerDelay)
    {
        this.dataClientAddDataPagesCircuitBreakerDelay = dataClientAddDataPagesCircuitBreakerDelay;
        return this;
    }
}