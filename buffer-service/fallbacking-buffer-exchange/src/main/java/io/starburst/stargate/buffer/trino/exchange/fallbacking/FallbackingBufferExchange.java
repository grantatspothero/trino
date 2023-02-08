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

import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class FallbackingBufferExchange
        implements Exchange
{
    private final Exchange exchange;
    private final FallbackSelector fallbackSelector;

    public FallbackingBufferExchange(Exchange exchange, FallbackSelector fallbackSelector)
    {
        this.exchange = requireNonNull(exchange, "exchange is null");
        this.fallbackSelector = requireNonNull(fallbackSelector, "fallbackSelector is null");
    }

    @Override
    public ExchangeId getId()
    {
        return fallbackSelector.handleCall(exchange::getId);
    }

    @Override
    public ExchangeSinkHandle addSink(int taskPartitionId)
    {
        return fallbackSelector.handleCall(() -> exchange.addSink(taskPartitionId));
    }

    @Override
    public void noMoreSinks()
    {
        fallbackSelector.handleCall(exchange::noMoreSinks);
    }

    @Override
    public CompletableFuture<ExchangeSinkInstanceHandle> instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        return fallbackSelector.handleCall(() -> exchange.instantiateSink(sinkHandle, taskAttemptId));
    }

    @Override
    public CompletableFuture<ExchangeSinkInstanceHandle> updateSinkInstanceHandle(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        return fallbackSelector.handleCall(() -> exchange.updateSinkInstanceHandle(sinkHandle, taskAttemptId));
    }

    @Override
    public void sinkFinished(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        fallbackSelector.handleCall(() -> exchange.sinkFinished(sinkHandle, taskAttemptId));
    }

    @Override
    public void allRequiredSinksFinished()
    {
        fallbackSelector.handleCall(exchange::allRequiredSinksFinished);
    }

    @Override
    public ExchangeSourceHandleSource getSourceHandles()
    {
        ExchangeSourceHandleSource sourceHandleSource = fallbackSelector.handleCall(exchange::getSourceHandles);
        return new FallbackingExchangeSourceHandleSource(sourceHandleSource, fallbackSelector);
    }

    @Override
    public void close()
    {
        fallbackSelector.handleCall(exchange::close);
    }
}
