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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeSourceHandle;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class LazyFallbackingExchangeSource
        implements ExchangeSource
{
    private final ExchangeManager bufferExchangeManager;
    private final ExchangeManager fileSystemExchangeManager;
    private final FallbackSelector fallbackSelector;

    private final AtomicReference<ExchangeSource> delegate = new AtomicReference<>();
    private final CompletableFuture<Void> initializationFuture = new CompletableFuture<>();
    @GuardedBy("this")
    private ExchangeSourceOutputSelector bufferedOutputSelector;
    private volatile boolean closed;

    public LazyFallbackingExchangeSource(
            ExchangeManager bufferExchangeManager,
            ExchangeManager fileSystemExchangeManager,
            FallbackSelector fallbackSelector)
    {
        this.bufferExchangeManager = requireNonNull(bufferExchangeManager, "bufferExchangeManager is null");
        this.fileSystemExchangeManager = requireNonNull(fileSystemExchangeManager, "fileSystemExchangeManager is null");
        this.fallbackSelector = requireNonNull(fallbackSelector, "fallbackSelector is null");
    }

    @Override
    public void addSourceHandles(List<ExchangeSourceHandle> handles)
    {
        boolean completeInitializationFuture = false;
        synchronized (this) {
            if (handles.isEmpty()) {
                return;
            }

            ExchangeSource delegateInstance = delegate.get();
            if (delegateInstance == null) {
                ExchangeSourceHandle handle = handles.get(0);
                if (handle instanceof BufferExchangeSourceHandle) {
                    delegateInstance = new FallbackingExchangeSource(bufferExchangeManager.createSource(), fallbackSelector);
                }
                else {
                    verify(handle instanceof FileSystemExchangeSourceHandle, "unexpected source handle %s", handle);
                    delegateInstance = fileSystemExchangeManager.createSource();
                }
                delegate.set(delegateInstance);

                if (bufferedOutputSelector != null) {
                    delegateInstance.setOutputSelector(bufferedOutputSelector);
                    bufferedOutputSelector = null;
                }
                completeInitializationFuture = true;
            }
            delegateInstance.addSourceHandles(handles);
        }
        if (completeInitializationFuture) {
            initializationFuture.complete(null);
        }
    }

    @Override
    public void noMoreSourceHandles()
    {
        boolean completeInitializationFuture = false;
        synchronized (this) {
            ExchangeSource delegateInstance = delegate.get();
            if (delegateInstance == null) {
                delegateInstance = new EmptyFinishedExchangeSource();
                delegate.set(delegateInstance);
                bufferedOutputSelector = null; // not important
                completeInitializationFuture = true;
            }
            delegateInstance.noMoreSourceHandles();
        }
        if (completeInitializationFuture) {
            initializationFuture.complete(null);
        }
    }

    @Override
    public void setOutputSelector(ExchangeSourceOutputSelector selector)
    {
        synchronized (this) {
            ExchangeSource delegateInstance = delegate.get();
            if (delegateInstance == null) {
                if (bufferedOutputSelector == null || bufferedOutputSelector.getVersion() < selector.getVersion()) {
                    bufferedOutputSelector = selector;
                }
                return;
            }
            delegateInstance.setOutputSelector(selector);
        }
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        if (this.closed) {
            return CompletableFuture.completedFuture(null);
        }
        if (!this.initializationFuture.isDone()) {
            // should use cancellation masking wrapper probably
            return this.initializationFuture;
        }
        return delegate.get().isBlocked();
    }

    @Override
    public boolean isFinished()
    {
        ExchangeSource delegateInstance = delegate.get();
        if (delegateInstance != null) {
            return delegateInstance.isFinished();
        }
        return closed;
    }

    @Nullable
    @Override
    public Slice read()
    {
        ExchangeSource delegateInstance = delegate.get();
        if (delegateInstance != null) {
            return delegateInstance.read();
        }
        return null;
    }

    @Override
    public long getMemoryUsage()
    {
        ExchangeSource delegateInstance = delegate.get();
        if (delegateInstance != null) {
            return delegateInstance.getMemoryUsage();
        }
        return 0;
    }

    @Override
    public void close()
    {
        ExchangeSource delegateInstance = delegate.get();
        if (delegateInstance != null) {
            delegateInstance.close();
            return;
        }
        this.closed = true;
    }
}
