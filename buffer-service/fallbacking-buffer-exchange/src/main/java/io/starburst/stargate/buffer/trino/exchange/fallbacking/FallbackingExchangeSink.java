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
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class FallbackingExchangeSink
        implements ExchangeSink
{
    private final ExchangeSink exchangeSink;
    private final FallbackSelector fallbackSelector;

    public FallbackingExchangeSink(ExchangeSink exchangeSink, FallbackSelector fallbackSelector)
    {
        this.exchangeSink = requireNonNull(exchangeSink, "exchangeSink is null");
        this.fallbackSelector = requireNonNull(fallbackSelector, "fallbackSelector is null");
    }

    @Override
    public boolean isHandleUpdateRequired()
    {
        return fallbackSelector.handleCall(exchangeSink::isHandleUpdateRequired);
    }

    @Override
    public void updateHandle(ExchangeSinkInstanceHandle handle)
    {
        fallbackSelector.handleCall(() -> exchangeSink.updateHandle(handle));
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        // not wrapping is blocked due to performance reasons
        return exchangeSink.isBlocked();
    }

    @Override
    public void add(int partitionId, Slice data)
    {
        fallbackSelector.handleCall(() -> exchangeSink.add(partitionId, data));
    }

    @Override
    public long getMemoryUsage()
    {
        return fallbackSelector.handleCall(exchangeSink::getMemoryUsage);
    }

    @Override
    public CompletableFuture<Void> finish()
    {
        return fallbackSelector.handleFutureCall(exchangeSink::finish);
    }

    @Override
    public CompletableFuture<Void> abort()
    {
        return fallbackSelector.handleFutureCall(exchangeSink::abort);
    }
}
