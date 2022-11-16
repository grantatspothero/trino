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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeManager;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeSinkInstanceHandle;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManager;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeSinkInstanceHandle;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class FallbackingExchangeManager
        implements ExchangeManager
{
    private static final Logger log = Logger.get(FallbackingExchangeManager.class);

    private static final Duration MAX_QUERY_RUNTIME = Duration.of(5, ChronoUnit.HOURS);

    private final ExchangeManager bufferExchangeManager;
    private final ExchangeManager fileSystemExchangeManager;
    private final FallbackSelector fallbackSelector;
    private final FallbackingExchangeStats stats;

    @GuardedBy("this")
    private final Map<QueryId, QueryExchangeModeInfo> queryExchangeModeInfos = new HashMap<>();

    @Inject
    public FallbackingExchangeManager(
            BufferExchangeManager bufferExchangeManager,
            FileSystemExchangeManager fileSystemExchangeManager,
            FallbackSelector fallbackSelector,
            FallbackingExchangeStats stats,
            ScheduledExecutorService executorService)
    {
        this.bufferExchangeManager = requireNonNull(bufferExchangeManager, "bufferExchangeManager is null");
        this.fileSystemExchangeManager = requireNonNull(fileSystemExchangeManager, "fileSystemExchangeManager is null");
        this.fallbackSelector = requireNonNull(fallbackSelector, "fallbackSelector is null");
        this.stats = requireNonNull(stats, "stats is null");
        executorService.scheduleWithFixedDelay(this::cleanupStaleQueryInfos, 0, 1, TimeUnit.MINUTES);
    }

    @Override
    public synchronized Exchange createExchange(ExchangeContext context, int outputPartitionCount, boolean preserveOrderWithinPartition)
    {
        QueryId queryId = context.getQueryId();
        Exchange exchange;
        try {
            exchange = switch (selectExchangeMode(queryId)) {
                case BUFFER -> {
                    stats.getBufferExchangesCreated().update(1);
                    yield new FallbackingBufferExchange(fallbackSelector.handleCall(() -> bufferExchangeManager.createExchange(context, outputPartitionCount, preserveOrderWithinPartition)), fallbackSelector);
                }
                // todo we cannot easily influence number of output partitions here, which makes replacing buffer exchange with filesystem exchange problematic
                case FILESYSTEM -> {
                    stats.getFilesystemExchangesCreated().update(1);
                    yield fileSystemExchangeManager.createExchange(context, outputPartitionCount, preserveOrderWithinPartition);
                }
            };
        }
        catch (Exception e) {
            cleanupQueryInfoIfNoExchanges(queryId);
            throw e;
        }

        // bump exchanges counter
        QueryExchangeModeInfo queryExchangeModeInfo = queryExchangeModeInfos.get(queryId);
        verify(queryExchangeModeInfo != null, "no entry for " + queryId);
        queryExchangeModeInfo.incrementExchangeCount();

        return new DelegatingExchange(exchange) {
            boolean closed;

            @Override
            public void close()
            {
                try {
                    synchronized (FallbackingExchangeManager.this) {
                        if (!closed) {
                            closed = true;
                            QueryExchangeModeInfo queryExchangeModeInfo = queryExchangeModeInfos.get(queryId);
                            verify(queryExchangeModeInfo != null, "no entry for " + queryId);
                            queryExchangeModeInfo.decrementExchangeCount();
                            cleanupQueryInfoIfNoExchanges(queryId);
                        }
                    }
                }
                finally {
                    super.close();
                }
            }
        };
    }

    private synchronized void cleanupQueryInfoIfNoExchanges(QueryId queryId)
    {
        QueryExchangeModeInfo queryExchangeModeInfo = queryExchangeModeInfos.get(queryId);
        if (queryExchangeModeInfo == null) {
            return;
        }
        if (queryExchangeModeInfo.getExchangesCount() == 0) {
            queryExchangeModeInfos.remove(queryId);
        }
    }

    private synchronized void cleanupStaleQueryInfos()
    {
        try {
            Iterator<Map.Entry<QueryId, QueryExchangeModeInfo>> iterator = queryExchangeModeInfos.entrySet().iterator();
            long stalenessThreshold = System.currentTimeMillis() - MAX_QUERY_RUNTIME.toMillis();
            while (iterator.hasNext()) {
                Map.Entry<QueryId, QueryExchangeModeInfo> entry = iterator.next();
                if (entry.getValue().getCreationTime() < stalenessThreshold) {
                    log.info("dropping stale entry for query " + entry.getKey());
                    iterator.remove();
                }
            }
        }
        catch (Exception e) {
            log.error(e, "error cleaning up state query info");
        }
    }

    private synchronized ExchangeMode selectExchangeMode(QueryId queryId)
    {
        QueryExchangeModeInfo queryExchangeModeInfo = queryExchangeModeInfos.get(queryId);
        if (queryExchangeModeInfo == null) {
            queryExchangeModeInfo = new QueryExchangeModeInfo(fallbackSelector.getExchangeMode());
            queryExchangeModeInfos.put(queryId, queryExchangeModeInfo);
        }
        return queryExchangeModeInfo.getExchangeMode();
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle)
    {
        if (handle instanceof BufferExchangeSinkInstanceHandle) {
            ExchangeSink exchangeSink = fallbackSelector.handleCall(() -> bufferExchangeManager.createSink(handle));
            return new FallbackingExchangeSink(exchangeSink, fallbackSelector);
        }
        verify(handle instanceof FileSystemExchangeSinkInstanceHandle, "unexpected sink instance handle %s", handle);
        return fileSystemExchangeManager.createSink(handle);
    }

    @Override
    public ExchangeSource createSource()
    {
        return new LazyFallbackingExchangeSource(bufferExchangeManager, fileSystemExchangeManager, fallbackSelector);
    }

    @VisibleForTesting
    public FallbackingExchangeStats.Snapshot getStats()
    {
        return stats.getSnapshot();
    }

    @Override
    public void shutdown()
    {
        Closer closer = Closer.create();
        closer.register(bufferExchangeManager::shutdown);
        closer.register(fileSystemExchangeManager::shutdown);
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class QueryExchangeModeInfo
    {
        private final ExchangeMode exchangeMode;
        private int exchangesCount;
        private final long creationTime;

        private QueryExchangeModeInfo(ExchangeMode exchangeMode)
        {
            this.exchangeMode = requireNonNull(exchangeMode, "exchangeMode is null");
            this.exchangesCount = 0;
            this.creationTime = System.currentTimeMillis();
        }

        public ExchangeMode getExchangeMode()
        {
            return exchangeMode;
        }

        public int getExchangesCount()
        {
            return exchangesCount;
        }

        public void incrementExchangeCount()
        {
            exchangesCount++;
        }

        public void decrementExchangeCount()
        {
            verify(exchangesCount > 0, "expected positive exchange count");
            exchangesCount--;
        }

        public long getCreationTime()
        {
            return creationTime;
        }
    }
}
