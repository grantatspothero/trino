/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig("chunk.spool-merge-enabled")
public class ChunkManagerConfig
{
    @VisibleForTesting
    static final Duration DEFAULT_EXCHANGE_STALENESS_THRESHOLD = succinctDuration(5, MINUTES);

    private DataSize chunkTargetSize = DataSize.of(16, MEGABYTE);
    private DataSize chunkMaxSize = DataSize.of(64, MEGABYTE);
    private DataSize chunkSliceSize = DataSize.of(128, KILOBYTE);
    private Duration exchangeStalenessThreshold = DEFAULT_EXCHANGE_STALENESS_THRESHOLD;
    private URI spoolingDirectory;
    private Duration chunkSpoolInterval = succinctDuration(50, MILLISECONDS);
    private int chunkSpoolConcurrency = 32;
    private int chunkSpoolMergeThreshold = 10;

    @NotNull
    @MinDataSize("4kB")
    @MaxDataSize("128MB")
    public DataSize getChunkTargetSize()
    {
        return chunkTargetSize;
    }

    @Config("chunk.target-size")
    @ConfigDescription("Target size of data that a chunk may accommodate. Should be a multiple of chunk.slice-size")
    public ChunkManagerConfig setChunkTargetSize(DataSize chunkTargetSize)
    {
        this.chunkTargetSize = chunkTargetSize;
        return this;
    }

    @NotNull
    @MinDataSize("16MB")
    @MaxDataSize("128MB")
    public DataSize getChunkMaxSize()
    {
        return chunkMaxSize;
    }

    @Config("chunk.max-size")
    @ConfigDescription("Max size of data that a chunk may accommodate. Should be a multiple of chunk.slice-size")
    public ChunkManagerConfig setChunkMaxSize(DataSize chunkMaxSize)
    {
        this.chunkMaxSize = chunkMaxSize;
        return this;
    }

    @NotNull
    @MinDataSize("4kB")
    @MaxDataSize("128MB")
    public DataSize getChunkSliceSize()
    {
        return chunkSliceSize;
    }

    @Config("chunk.slice-size")
    @ConfigDescription("Size of a chunk slice in an adaptively allocated chunk")
    public ChunkManagerConfig setChunkSliceSize(DataSize chunkSliceSize)
    {
        this.chunkSliceSize = chunkSliceSize;
        return this;
    }

    @NotNull
    public Duration getExchangeStalenessThreshold()
    {
        return exchangeStalenessThreshold;
    }

    @Config("exchange.staleness-threshold")
    public ChunkManagerConfig setExchangeStalenessThreshold(Duration exchangeStalenessThreshold)
    {
        this.exchangeStalenessThreshold = exchangeStalenessThreshold;
        return this;
    }

    @NotNull
    public URI getSpoolingDirectory()
    {
        return spoolingDirectory;
    }

    @Config("spooling.directory")
    public ChunkManagerConfig setSpoolingDirectory(String spoolingDirectory)
    {
        if (spoolingDirectory != null) {
            if (!spoolingDirectory.endsWith(PATH_SEPARATOR)) {
                spoolingDirectory += PATH_SEPARATOR;
            }
            this.spoolingDirectory = URI.create(spoolingDirectory);
        }
        return this;
    }

    @NotNull
    public Duration getChunkSpoolInterval()
    {
        return chunkSpoolInterval;
    }

    @Config("chunk.spool-interval")
    public ChunkManagerConfig setChunkSpoolInterval(Duration chunkSpoolInterval)
    {
        this.chunkSpoolInterval = chunkSpoolInterval;
        return this;
    }

    @Min(1)
    public int getChunkSpoolConcurrency()
    {
        return chunkSpoolConcurrency;
    }

    @Config("chunk.spool-concurrency")
    public ChunkManagerConfig setChunkSpoolConcurrency(int chunkSpoolConcurrency)
    {
        this.chunkSpoolConcurrency = chunkSpoolConcurrency;
        return this;
    }

    public int getChunkSpoolMergeThreshold()
    {
        return chunkSpoolMergeThreshold;
    }

    @Config("chunk.spool-merge-threshold")
    public ChunkManagerConfig setChunkSpoolMergeThreshold(int chunkSpoolMergeThreshold)
    {
        this.chunkSpoolMergeThreshold = chunkSpoolMergeThreshold;
        return this;
    }

    @PostConstruct
    public void validate()
    {
        checkState(chunkTargetSize.toBytes() % chunkSliceSize.toBytes() == 0, "chunk.target-size must be a multiple of chunk.slice-size");
        checkState(chunkMaxSize.toBytes() % chunkSliceSize.toBytes() == 0, "chunk.max-size must be a multiple of chunk.slice-size");
        checkState(chunkMaxSize.compareTo(chunkTargetSize) >= 0, "chunk.max-size must not be smaller than chunk.target-size");
    }
}
