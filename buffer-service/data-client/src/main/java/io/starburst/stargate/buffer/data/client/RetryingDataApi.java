/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class RetryingDataApi
        implements DataApi
{
    private static final Logger logger = Logger.get(RetryingDataApi.class);

    private final DataApi delegate;
    private final ScheduledExecutorService executor;

    private final RetryPolicy<Object> retryPolicy;

    public RetryingDataApi(
            DataApi delegate,
            int maxRetries,
            Duration backoffInitial,
            Duration backoffMax,
            double backoffFactor,
            double backoffJitter,
            ScheduledExecutorService executor)
    {
        this.delegate = delegate;
        this.executor = requireNonNull(executor, "executor is null");
        this.retryPolicy = RetryPolicy.builder()
                .withBackoff(
                        java.time.Duration.ofMillis(backoffInitial.toMillis()),
                        java.time.Duration.ofMillis(backoffMax.toMillis()),
                        backoffFactor)
                .withMaxRetries(maxRetries)
                .withJitter(backoffJitter)
                .onRetry(event -> logger.warn(event.getLastException(), "retrying DataApi request (%s)".formatted(event.getAttemptCount())))
                .handleIf(throwable -> {
                    if (!(throwable instanceof DataApiException dataApiException)) {
                        return true;
                    }
                    return dataApiException.getErrorCode() == ErrorCode.INTERNAL_ERROR;
                })
                .build();
    }

    @Override
    public BufferNodeInfo getInfo()
    {
        return getRetryExecutor()
                .get(execution -> delegate.getInfo());
    }

    @Override
    public ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        return runWithRetry(() -> delegate.listClosedChunks(exchangeId, pagingId));
    }

    @Override
    public ListenableFuture<Void> markAllClosedChunksReceived(String exchangeId)
    {
        return runWithRetry(() -> delegate.markAllClosedChunksReceived(exchangeId));
    }

    @Override
    public ListenableFuture<Void> registerExchange(String exchangeId)
    {
        return runWithRetry(() -> delegate.registerExchange(exchangeId));
    }

    @Override
    public ListenableFuture<Void> pingExchange(String exchangeId)
    {
        return runWithRetry(() -> delegate.pingExchange(exchangeId));
    }

    @Override
    public ListenableFuture<Void> removeExchange(String exchangeId)
    {
        return runWithRetry(() -> delegate.removeExchange(exchangeId));
    }

    @Override
    public ListenableFuture<Void> addDataPages(String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition)
    {
        AtomicBoolean retryFlag = new AtomicBoolean();
        Callable<ListenableFuture<Void>> call = () -> {
            boolean retry = retryFlag.getAndSet(true);

            ListenableFuture<Void> future = delegate.addDataPages(exchangeId, taskId, attemptId, dataPagesId, dataPagesByPartition);
            if (!retry) {
                // first try
                return future;
            }

            // If we are retrying we need to ensure that we do not propagate DRAINING error to user. We do not know if previous request
            // was recorded by server or not. If we handle DRAINING, and send data to another buffer service node we may end up with
            // duplicated data.
            SettableFuture<Void> resultFuture = SettableFuture.create();
            Futures.addCallback(future, new FutureCallback<>()
            {
                @Override
                public void onSuccess(Void result)
                {
                    resultFuture.set(result);
                }

                @Override
                public void onFailure(Throwable failure)
                {
                    if ((failure instanceof DataApiException dataApiException) && dataApiException.getErrorCode() == ErrorCode.DRAINING) {
                        resultFuture.setException(new DataApiException(ErrorCode.USER_ERROR, "Translating DRAINING to USER_ERROR on retry", failure));
                        return;
                    }
                    resultFuture.setException(failure);
                }
            }, directExecutor());

            return resultFuture;
        };
        return runWithRetry(call);
    }

    @Override
    public ListenableFuture<Void> finishExchange(String exchangeId)
    {
        return runWithRetry(() -> delegate.finishExchange(exchangeId));
    }

    @Override
    public ListenableFuture<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
    {
        return runWithRetry(() -> delegate.getChunkData(bufferNodeId, exchangeId, partitionId, chunkId));
    }

    private <T> ListenableFuture<T> runWithRetry(Callable<ListenableFuture<T>> routine)
    {
        CompletableFuture<T> finalFuture = getRetryExecutor()
                .getAsyncExecution(execution -> {
                    ListenableFuture<T> future = routine.call();
                    Futures.addCallback(future, new FutureCallback<>()
                    {
                        @Override
                        public void onSuccess(T result)
                        {
                            execution.recordResult(result);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            execution.recordException(t);
                        }
                    }, directExecutor());
                });

        return MoreFutures.toListenableFuture(finalFuture);
    }

    private FailsafeExecutor<Object> getRetryExecutor()
    {
        return Failsafe.with(retryPolicy)
                .with(executor);
    }
}
