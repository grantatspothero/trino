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
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class FallbackingExchangeSource
        implements ExchangeSource
{
    private final ExchangeSource exchangeSource;
    private final FallbackSelector fallbackSelector;

    public FallbackingExchangeSource(ExchangeSource exchangeSource, FallbackSelector fallbackSelector)
    {
        this.exchangeSource = requireNonNull(exchangeSource, "exchangeSource is null");
        this.fallbackSelector = requireNonNull(fallbackSelector, "fallbackSelector is null");
    }

    @Override
    public void addSourceHandles(List<ExchangeSourceHandle> handles)
    {
        fallbackSelector.handleCall(() -> exchangeSource.addSourceHandles(handles));
    }

    @Override
    public void noMoreSourceHandles()
    {
        fallbackSelector.handleCall(exchangeSource::noMoreSourceHandles);
    }

    @Override
    public void setOutputSelector(ExchangeSourceOutputSelector selector)
    {
        fallbackSelector.handleCall(() -> exchangeSource.setOutputSelector(selector));
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        // not wrapping due to performance reasons
        return exchangeSource.isBlocked();
    }

    @Override
    public boolean isFinished()
    {
        return fallbackSelector.handleCall(exchangeSource::isFinished);
    }

    @Nullable
    @Override
    public Slice read()
    {
        return fallbackSelector.handleCall(exchangeSource::read);
    }

    @Override
    public long getMemoryUsage()
    {
        return fallbackSelector.handleCall(exchangeSource::getMemoryUsage);
    }

    @Override
    public void close()
    {
        fallbackSelector.handleCall(exchangeSource::close);
    }
}
