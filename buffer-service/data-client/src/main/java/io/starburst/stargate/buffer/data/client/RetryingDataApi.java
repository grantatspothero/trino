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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.concurrent.MoreFutures;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class RetryingDataApi
        implements DataApi
{
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
                .handleIf(throwable -> {
                    if (!(throwable instanceof DataApiException dataApiException)) {
                        return true;
                    }
                    return dataApiException.getErrorCode() == ErrorCode.INTERNAL_ERROR;
                })
                .build();
    }

    @Override
    public ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
    {
        return runWithRetry(() -> delegate.listClosedChunks(exchangeId, pagingId));
    }

    @Override
    public ListenableFuture<Void> registerExchange(String exchangeId)
    {
        return runWithRetry(() -> delegate.registerExchange(exchangeId));
    }

    @Override
    public ListenableFuture<Void> pingExchange(String exchangeId)
    {
        return runWithRetry(() -> pingExchange(exchangeId));
    }

    @Override
    public ListenableFuture<Void> removeExchange(String exchangeId)
    {
        return runWithRetry(() -> removeExchange(exchangeId));
    }

    @Override
    public ListenableFuture<Void> addDataPages(String exchangeId, int partitionId, int taskId, int attemptId, long dataPagesId, List<Slice> dataPages)
    {
        return runWithRetry(() -> addDataPages(exchangeId, partitionId, taskId, attemptId, dataPagesId, dataPages));
    }

    @Override
    public ListenableFuture<Void> finishExchange(String exchangeId)
    {
        return runWithRetry(() -> finishExchange(exchangeId));
    }

    @Override
    public ListenableFuture<List<DataPage>> getChunkData(String exchangeId, int partitionId, long chunkId, long bufferNodeId)
    {
        return runWithRetry(() -> getChunkData(exchangeId, partitionId, chunkId, bufferNodeId));
    }

    private <T> ListenableFuture<T> runWithRetry(Callable<ListenableFuture<T>> routine)
    {
        CompletableFuture<T> finalFuture =
                Failsafe.with(retryPolicy)
                        .with(executor)
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
}
