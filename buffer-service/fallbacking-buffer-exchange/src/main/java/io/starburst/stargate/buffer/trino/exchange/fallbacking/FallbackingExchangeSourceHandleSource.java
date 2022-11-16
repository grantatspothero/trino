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

import io.trino.spi.exchange.ExchangeSourceHandleSource;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class FallbackingExchangeSourceHandleSource
        implements ExchangeSourceHandleSource
{
    private final ExchangeSourceHandleSource sourceHandleSource;
    private final FallbackSelector fallbackSelector;

    public FallbackingExchangeSourceHandleSource(ExchangeSourceHandleSource sourceHandleSource, FallbackSelector fallbackSelector)
    {
        this.sourceHandleSource = requireNonNull(sourceHandleSource, "sourceHandleSource is null");
        this.fallbackSelector = requireNonNull(fallbackSelector, "fallbackSelector is null");
    }

    @Override
    public CompletableFuture<ExchangeSourceHandleBatch> getNextBatch()
    {
        return fallbackSelector.handleFutureCall(sourceHandleSource::getNextBatch);
    }

    @Override
    public void close()
    {
        fallbackSelector.handleCall(sourceHandleSource::close);
    }
}
