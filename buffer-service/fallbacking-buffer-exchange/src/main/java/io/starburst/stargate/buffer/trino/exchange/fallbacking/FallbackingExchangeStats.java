/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange.fallbacking;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class FallbackingExchangeStats
{
    private final CounterStat bufferExchangesCreated = new CounterStat();
    private final CounterStat filesystemExchangesCreated = new CounterStat();

    @Managed
    @Nested
    public CounterStat getBufferExchangesCreated()
    {
        return bufferExchangesCreated;
    }

    @Managed
    @Nested
    public CounterStat getFilesystemExchangesCreated()
    {
        return filesystemExchangesCreated;
    }

    public Snapshot getSnapshot()
    {
        return new Snapshot(bufferExchangesCreated.getTotalCount(), filesystemExchangesCreated.getTotalCount());
    }

    public record Snapshot(long bufferExchangesCreated, long filesystemExchangesCreated) {}
}
