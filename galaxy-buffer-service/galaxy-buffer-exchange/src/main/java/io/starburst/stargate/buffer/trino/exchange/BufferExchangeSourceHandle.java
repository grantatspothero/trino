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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class BufferExchangeSourceHandle
        implements ExchangeSourceHandle
{
    private static final int INSTANCE_SIZE = instanceSize(BufferExchangeSourceHandle.class);

    private final String externalExchangeId;
    private final int partitionId;
    private final long[] bufferNodeIds;
    private final long[] chunkIds;
    private final int[] chunkDataSizes;
    private final long dataSizeInBytes;
    private final boolean preserveOrderWithinPartition;

    @JsonCreator
    @Deprecated // for Jackson and internal use
    public BufferExchangeSourceHandle(
            @JsonProperty("externalExchangeId") String externalExchangeId,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("bufferNodeIds") long[] bufferNodeIds,
            @JsonProperty("chunkIds") long[] chunkIds,
            @JsonProperty("chunkDataSizes") int[] chunkDataSizes,
            @JsonProperty("dataSizeInBytes") long dataSizeInBytes,
            @JsonProperty("preserveOrderWithinPartition") boolean preserveOrderWithinPartition)
    {
        this.externalExchangeId = requireNonNull(externalExchangeId, "externalExchangeId is null");
        this.partitionId = partitionId;
        this.bufferNodeIds = requireNonNull(bufferNodeIds, "bufferNodeIds is null");
        this.chunkIds = requireNonNull(chunkIds, "chunkIds is null");
        checkArgument(chunkIds.length == bufferNodeIds.length, "length of chunkIds(%s) and bufferNodeIds(%s) should be equal", chunkIds.length, bufferNodeIds.length);
        this.chunkDataSizes = requireNonNull(chunkDataSizes, "chunkDataSizes is null");
        checkArgument(chunkDataSizes.length == chunkIds.length, "length of chunkDataSizes(%s) and chunkIds(%s) should be equal", chunkDataSizes.length, chunkIds.length);
        this.dataSizeInBytes = dataSizeInBytes;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;
    }

    public static BufferExchangeSourceHandle fromChunkHandles(
            String externalExchangeId,
            int partitionId,
            List<ChunkHandle> chunkHandles,
            boolean preserveOrderWithinPartition)
    {
        long[] bufferNodeIds = new long[chunkHandles.size()];
        long[] chunkIds = new long[chunkHandles.size()];
        int[] chunkDataSizes = new int[chunkHandles.size()];
        long dataSizeInBytes = 0;
        int pos = 0;
        for (ChunkHandle chunkHandle : chunkHandles) {
            checkArgument(chunkHandle.partitionId() == partitionId, "all chunkHandles should belong to same partition %s; got %s", partitionId, chunkHandle.partitionId());
            bufferNodeIds[pos] = chunkHandle.bufferNodeId();
            chunkIds[pos] = chunkHandle.chunkId();
            chunkDataSizes[pos] = chunkHandle.dataSizeInBytes();
            dataSizeInBytes += chunkHandle.dataSizeInBytes();
            pos++;
        }

        return new BufferExchangeSourceHandle(
                externalExchangeId,
                partitionId,
                bufferNodeIds,
                chunkIds,
                chunkDataSizes,
                dataSizeInBytes,
                preserveOrderWithinPartition);
    }

    @JsonProperty
    public String getExternalExchangeId()
    {
        return externalExchangeId;
    }

    @Override
    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    @Deprecated // needed for JSON serialization
    public long[] getBufferNodeIds()
    {
        return bufferNodeIds;
    }

    @JsonProperty
    @Deprecated // needed for JSON serialization
    public long[] getChunkIds()
    {
        return chunkIds;
    }

    @JsonProperty
    @Deprecated // needed for JSON serialization
    public int[] getChunkDataSizes()
    {
        return chunkDataSizes;
    }

    public int getChunksCount()
    {
        return bufferNodeIds.length;
    }

    public long getBufferNodeId(int pos)
    {
        checkPositionIndex(pos, bufferNodeIds.length);
        return bufferNodeIds[pos];
    }

    public long getChunkId(int pos)
    {
        checkPositionIndex(pos, chunkIds.length);
        return chunkIds[pos];
    }

    public int getChunkDataSize(int pos)
    {
        checkPositionIndex(pos, chunkDataSizes.length);
        return chunkDataSizes[pos];
    }

    @Override
    @JsonProperty
    public long getDataSizeInBytes()
    {
        return dataSizeInBytes;
    }

    @JsonProperty
    public boolean isPreserveOrderWithinPartition()
    {
        return preserveOrderWithinPartition;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(externalExchangeId)
                + sizeOf(bufferNodeIds)
                + sizeOf(chunkIds)
                + sizeOf(chunkDataSizes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("externalExchangeId", externalExchangeId)
                .add("partitionId", partitionId)
                .add("bufferNodeIds", bufferNodeIds)
                .add("chunkIds", chunkIds)
                .add("chunkDataSizes", chunkDataSizes)
                .add("dataSizeInBytes", dataSizeInBytes)
                .toString();
    }
}