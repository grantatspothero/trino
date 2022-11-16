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

import static java.util.Objects.requireNonNull;

public class DelegatingExchange
        implements Exchange
{
    private final Exchange delegate;

    public DelegatingExchange(Exchange delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ExchangeId getId()
    {
        return delegate.getId();
    }

    @Override
    public ExchangeSinkHandle addSink(int taskPartitionId)
    {
        return delegate.addSink(taskPartitionId);
    }

    @Override
    public void noMoreSinks()
    {
        delegate.noMoreSinks();
    }

    @Override
    public ExchangeSinkInstanceHandle instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        return delegate.instantiateSink(sinkHandle, taskAttemptId);
    }

    @Override
    public ExchangeSinkInstanceHandle updateSinkInstanceHandle(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        return updateSinkInstanceHandle(sinkHandle, taskAttemptId);
    }

    @Override
    public void sinkFinished(ExchangeSinkHandle sinkHandle, int taskAttemptId)
    {
        delegate.sinkFinished(sinkHandle, taskAttemptId);
    }

    @Override
    public void allRequiredSinksFinished()
    {
        delegate.allRequiredSinksFinished();
    }

    @Override
    public ExchangeSourceHandleSource getSourceHandles()
    {
        return delegate.getSourceHandles();
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
