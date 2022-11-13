/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling;

import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.data.execution.ChunkDataHolder;
import io.starburst.stargate.buffer.data.memory.SliceLease;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static java.util.Objects.requireNonNull;

public class ChunkDataLease
{
    @Nullable
    private final SliceLease sliceLease;
    private final ListenableFuture<ChunkDataHolder> chunkDataHolderFuture;

    public ChunkDataLease(
            @Nullable SliceLease sliceLease,
            ListenableFuture<ChunkDataHolder> chunkDataHolderFuture)
    {
        this.sliceLease = sliceLease;
        this.chunkDataHolderFuture = requireNonNull(chunkDataHolderFuture, "chunkDataHolderFuture is null");
    }

    public ListenableFuture<ChunkDataHolder> getChunkDataHolderFuture()
    {
        return nonCancellationPropagating(chunkDataHolderFuture);
    }

    public void release()
    {
        checkState(chunkDataHolderFuture.isDone(), "chunkDataHolderFuture is not done");
        if (sliceLease != null) {
            sliceLease.release();
        }
    }
}
