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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.starburst.stargate.buffer.data.client.DataPage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.toChunkDataLease;

public class TestChunkManagerSpoolMergeDisabled
        extends AbstractTestChunkManager
{
    @Override
    protected boolean isChunkSpoolMergeEnabled()
    {
        return false;
    }

    @Test
    public void testGetDrainedChunkDataOnAnotherNode()
    {
        long drainedBufferNodeId = BUFFER_NODE_ID + 1;
        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice("test")),
                new DataPage(0, 1, utf8Slice("Spooling")),
                new DataPage(1, 0, utf8Slice("Storage")));
        getFutureValue(spoolingStorage.writeChunk(drainedBufferNodeId, EXCHANGE_0, 0L, toChunkDataLease(dataPages)));

        ChunkManager chunkManager = createChunkManager(
                defaultMemoryAllocator(),
                DataSize.of(16, MEGABYTE),
                DataSize.of(64, MEGABYTE),
                DataSize.of(128, KILOBYTE));

        // exchange missing
        assertDrainedChunkDataResult(chunkManager, drainedBufferNodeId);

        // exchange exists, but partition missing
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 1, 1, 1, 1L, ImmutableList.of(utf8Slice("dummy1"))).addDataPagesFuture());
        assertDrainedChunkDataResult(chunkManager, drainedBufferNodeId);

        // exchange exists, partition exists, but chunk missing
        getFutureValue(chunkManager.addDataPages(EXCHANGE_0, 0, 0, 0, 0L, ImmutableList.of(utf8Slice("dummy0"))).addDataPagesFuture());
        assertDrainedChunkDataResult(chunkManager, drainedBufferNodeId);

        chunkManager.removeExchange(EXCHANGE_0);
    }
}
