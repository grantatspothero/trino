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

import io.airlift.slice.Slice;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class EmptyFinishedExchangeSource
        implements ExchangeSource
{
    @Override
    public void addSourceHandles(List<ExchangeSourceHandle> handles)
    {
        throw new IllegalArgumentException("cannot add source handles");
    }

    @Override
    public void noMoreSourceHandles()
    {
        // ignore
    }

    @Override
    public void setOutputSelector(ExchangeSourceOutputSelector selector)
    {
        // ignore
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isFinished()
    {
        return true;
    }

    @Nullable
    @Override
    public Slice read()
    {
        return null;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        // ignore
    }
}
