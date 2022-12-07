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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestRetryingDataApi
{
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(4);

    @AfterAll
    public void teardown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testRetriesWithDelay()
            throws InterruptedException
    {
        TestingDataApi delegate = new TestingDataApi();
        DataApi retryingDataApi = new RetryingDataApi(delegate, 2, Duration.valueOf("500ms"), Duration.valueOf("1000ms"), 2.0, 0.0, executor);

        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new RuntimeException("random exception")));
        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new RuntimeException("random exception")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFuture(result));

        ListenableFuture<ChunkList> future = retryingDataApi.listClosedChunks("exchange-1", OptionalLong.empty());

        assertThat(delegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(1);
        Thread.sleep(550);
        assertThat(delegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(2);
        Thread.sleep(1000);
        assertThat(delegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(3);
        assertThat(future).isDone();
    }

    @Test
    public void testRetriesOnRuntimeException()
            throws ExecutionException
    {
        TestingDataApi delegate = new TestingDataApi();
        DataApi retryingDataApi = new RetryingDataApi(delegate, 2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, executor);

        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new RuntimeException("random exception")));
        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new RuntimeException("random exception")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFuture(result));

        ListenableFuture<ChunkList> future = retryingDataApi.listClosedChunks("exchange-1", OptionalLong.empty());

        assertThat(future).succeedsWithin(5, SECONDS);
        assertThat(Futures.getDone(future)).isEqualTo(result);
        assertThat(delegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(3);
    }

    @Test
    public void testRetriesOnInternalErrorException()
            throws ExecutionException
    {
        TestingDataApi delegate = new TestingDataApi();
        DataApi retryingDataApi = new RetryingDataApi(delegate, 2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, executor);

        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new DataApiException(ErrorCode.INTERNAL_ERROR, "blah")));
        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFuture(result));

        ListenableFuture<ChunkList> future = retryingDataApi.listClosedChunks("exchange-1", OptionalLong.empty());

        assertThat(future).succeedsWithin(5, SECONDS);
        assertThat(Futures.getDone(future)).isEqualTo(result);
        assertThat(delegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(3);
    }

    @ParameterizedTest
    @EnumSource(value = ErrorCode.class)
    public void testDoNotRetryOnMostDataApiExceptions(ErrorCode errorCode)
    {
        if (errorCode == ErrorCode.INTERNAL_ERROR) {
            return; // skip
        }
        TestingDataApi delegate = new TestingDataApi();
        DataApi retryingDataApi = new RetryingDataApi(delegate, 2, Duration.valueOf("1ms"), Duration.valueOf("2ms"), 2.0, 0.0, executor);

        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFailedFuture(new DataApiException(errorCode, "blah")));
        ListenableFuture<ChunkList> future = retryingDataApi.listClosedChunks("exchange-1", OptionalLong.empty());

        assertThat(future).failsWithin(1, SECONDS);

        assertThatThrownBy(() -> Futures.getUnchecked(future))
                .isInstanceOf(UncheckedExecutionException.class)
                .matches(e -> ((DataApiException) e.getCause()).getErrorCode().equals(errorCode));
        assertThat(delegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(1);
    }

    @Test
    public void testDoNotRetryOnSuccess()
            throws ExecutionException
    {
        TestingDataApi delegate = new TestingDataApi();
        DataApi retryingDataApi = new RetryingDataApi(delegate, 2, Duration.valueOf("1000ms"), Duration.valueOf("2000ms"), 2.0, 0.0, executor);

        ChunkList result = new ChunkList(ImmutableList.of(), OptionalLong.of(7));
        delegate.recordListClosedChunks("exchange-1", OptionalLong.empty(), Futures.immediateFuture(result));
        ListenableFuture<ChunkList> future = retryingDataApi.listClosedChunks("exchange-1", OptionalLong.empty());

        assertThat(future).succeedsWithin(1, MILLISECONDS); // returns immediately

        assertThat(Futures.getDone(future)).isEqualTo(result);
        assertThat(delegate.getListClosedChunksCallCount("exchange-1", OptionalLong.empty())).isEqualTo(1);
    }

    private static class TestingDataApi
            implements DataApi
    {
        private record ListClosedChunksKey(String exchangeId, OptionalLong pagingId) {};

        private final ListMultimap<ListClosedChunksKey, ListenableFuture<ChunkList>> listClosedChunksResponses = ArrayListMultimap.create();
        private final Map<ListClosedChunksKey, AtomicLong> listClosedChunksCounters = new HashMap<>();

        @Override
        public BufferNodeInfo getInfo()
        {
            return new BufferNodeInfo(1, URI.create("http://testing"), Optional.empty(), BufferNodeState.ACTIVE);
        }

        @Override
        public synchronized ListenableFuture<ChunkList> listClosedChunks(String exchangeId, OptionalLong pagingId)
        {
            ListClosedChunksKey key = new ListClosedChunksKey(exchangeId, pagingId);
            List<ListenableFuture<ChunkList>> responses = listClosedChunksResponses.get(key);
            if (responses.isEmpty()) {
                throw new IllegalStateException("no response recorded for " + key);
            }
            listClosedChunksCounters.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
            return responses.remove(0);
        }

        public synchronized void recordListClosedChunks(String exchangeId, OptionalLong pagingId, ListenableFuture<ChunkList> result)
        {
            listClosedChunksResponses.put(new ListClosedChunksKey(exchangeId, pagingId), result);
        }

        public synchronized long getListClosedChunksCallCount(String exchangeId, OptionalLong pagingId)
        {
            return listClosedChunksCounters.computeIfAbsent(new ListClosedChunksKey(exchangeId, pagingId), ignored -> new AtomicLong()).get();
        }

        @Override
        public synchronized ListenableFuture<Void> registerExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Void> pingExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Void> removeExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Void> addDataPages(String exchangeId, int taskId, int attemptId, long dataPagesId, ListMultimap<Integer, Slice> dataPagesByPartition)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<Void> finishExchange(String exchangeId)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public synchronized ListenableFuture<List<DataPage>> getChunkData(long bufferNodeId, String exchangeId, int partitionId, long chunkId)
        {
            throw new RuntimeException("not implemented");
        }
    }
}
