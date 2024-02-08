/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.metadataonly;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.starburst.stargate.id.AccountId;
import io.trino.NotInTransactionException;
import io.trino.connector.CatalogConnector;
import io.trino.connector.CatalogFactory;
import io.trino.connector.CatalogProperties;
import io.trino.connector.ConnectorName;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogHandleType;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class CachingCatalogFactory
        implements CatalogFactory
{
    private static final Logger log = Logger.get(CachingCatalogFactory.class);

    private final CatalogFactory catalogFactory;
    private final long cacheDurationNanos;
    private final Map<Key, Entry> cache;
    private final Map<CatalogConnector, Entry> wrappedConnectorIndex;
    private final boolean enabled;
    private final Optional<AccountContext> accountContext;
    private final ScheduledExecutorService executorService;

    private record Key(AccountId accountId, String catalogName, CatalogHandleType type, Map<String, String> decryptedProperties)
    {
        private Key
        {
            requireNonNull(accountId, "accountId is null");
            requireNonNull(catalogName, "catalogName is null");
            requireNonNull(type, "type is null");
            requireNonNull(decryptedProperties, "decryptedProperties is null");
        }
    }

    private static class Entry
    {
        private final Key key;
        @GuardedBy("synchronized(entry)") @Nullable private CatalogConnector catalogConnector;
        @GuardedBy("synchronized(entry)") private int useCount;
        @GuardedBy("synchronized(entry)") private long lastUse;
        @GuardedBy("synchronized(entry)") private boolean deleted;

        private Entry(Key key)
        {
            this.key = requireNonNull(key, "key is null");
            this.lastUse = Ticker.systemTicker().read();
        }
    }

    @Inject
    public CachingCatalogFactory(CatalogFactory catalogFactory, MetadataOnlyConfig metadataOnlyConfig)
    {
        this(catalogFactory,
                metadataOnlyConfig.getConnectorCacheDuration().roundTo(NANOSECONDS),
                new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(),
                Optional.empty(),
                newSingleThreadScheduledExecutor(threadsNamed("caching-catalog-factory-cleaner-%s")));
    }

    private CachingCatalogFactory(
            CatalogFactory catalogFactory,
            long cacheDurationNanos,
            Map<Key, Entry> cache,
            Map<CatalogConnector, Entry> wrappedConnectorIndex,
            Optional<AccountContext> accountContext,
            ScheduledExecutorService executorService)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.cache = requireNonNull(cache, "cache is null");
        this.wrappedConnectorIndex = requireNonNull(wrappedConnectorIndex, "wrappedConnectorIndex is null");
        this.accountContext = requireNonNull(accountContext, "accountContext is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.cacheDurationNanos = cacheDurationNanos;

        enabled = cacheDurationNanos > 0;
    }

    @PostConstruct
    public void start()
    {
        if (enabled) {
            long delayNanos = Math.max(TimeUnit.MILLISECONDS.toNanos(1), cacheDurationNanos / 3);
            executorService.scheduleWithFixedDelay(() -> cleanCache(cacheDurationNanos), delayNanos, delayNanos, NANOSECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        shutdownAndAwaitTermination(executorService, Duration.ofSeconds(10));

        cleanCache(-1);

        int size = cacheSize();
        if (size > 0) {
            log.warn("shutdown left %s connectors still cached", size);
        }
    }

    @VisibleForTesting
    public int cacheSize()
    {
        return cache.size();
    }

    @Override
    public void addConnectorFactory(ConnectorFactory connectorFactory)
    {
        catalogFactory.addConnectorFactory(connectorFactory);
    }

    @Override
    public CatalogConnector createCatalog(CatalogProperties catalogProperties)
    {
        Map<String, String> decryptedCatalogProperties = accountContext().decryptCatalogProperties(catalogProperties.getCatalogHandle().getCatalogName());
        return getOrBuildCatalogConnector(catalogProperties.getCatalogHandle(), Optional.of(catalogProperties), keyFor(catalogProperties, decryptedCatalogProperties), () ->
                catalogFactory.createCatalog(new CatalogProperties(catalogProperties.getCatalogHandle(), catalogProperties.getConnectorName(), decryptedCatalogProperties)));
    }

    @Override
    public CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector)
    {
        if (catalogHandle.getCatalogName().equals(GlobalSystemConnector.NAME)) {
            return catalogFactory.createCatalog(catalogHandle, connectorName, connector);
        }

        return getOrBuildCatalogConnector(catalogHandle, Optional.empty(), keyFor(catalogHandle, connectorName), () -> catalogFactory.createCatalog(catalogHandle, connectorName, connector));
    }

    CachingCatalogFactory withAccountContext(AccountContext accountContext)
    {
        return new CachingCatalogFactory(catalogFactory, cacheDurationNanos, cache, wrappedConnectorIndex, Optional.of(accountContext), executorService);
    }

    void releaseCatalogConnector(CatalogConnector catalogConnector)
    {
        if (!enabled) {
            shutdownConnector(catalogConnector);
            return;
        }

        if (catalogConnector.getCatalog().getCatalogName().equals(GlobalSystemConnector.NAME)) {
            shutdownConnector(catalogConnector);
            return;
        }

        Entry entry = wrappedConnectorIndex.remove(catalogConnector);
        if (entry == null) {
            throw new NotInTransactionException();
        }

        synchronized (entry) {
            entry.useCount--;
            if (entry.useCount < 0) {
                throw new IllegalStateException("Cached connector use count has gone negative. value: %s; key: %s".formatted(entry.useCount, entry.key));
            }
        }
    }

    private CatalogConnector getOrBuildCatalogConnector(CatalogHandle catalogHandle, Optional<CatalogProperties> catalogProperties, Key key, Supplier<CatalogConnector> catalogConnectorSupplier)
    {
        if (!enabled) {
            return catalogConnectorSupplier.get();
        }

        CatalogConnector catalogConnector = null;
        Entry entry = null;
        while (catalogConnector == null) {
            entry = cache.computeIfAbsent(key, ignore -> new Entry(key));
            synchronized (entry) {
                if (entry.deleted) {
                    continue;
                }

                if (entry.catalogConnector == null) {
                    entry.catalogConnector = catalogConnectorSupplier.get();
                }

                ++entry.useCount;
                entry.lastUse = Ticker.systemTicker().read();
                catalogConnector = entry.catalogConnector;
            }
        }

        Connector connectorToCache = catalogConnector.getMaterializedConnector(CatalogHandleType.NORMAL).getConnector();
        CatalogConnector wrappedConnector = catalogFactory.createCatalog(catalogHandle, catalogConnector.getConnectorName(), connectorToCache, catalogProperties);
        if (wrappedConnectorIndex.put(wrappedConnector, entry) != null) {
            throw new IllegalStateException("Wrapped connector already in index map for key: " + key);
        }

        return wrappedConnector;
    }

    private Key keyFor(CatalogProperties catalogProperties, Map<String, String> decryptedProperties)
    {
        return new Key(accountContext().accountId(), "CATALOG_" + catalogProperties.getCatalogHandle().getCatalogName(), catalogProperties.getCatalogHandle().getType(), ImmutableMap.copyOf(decryptedProperties));
    }

    private Key keyFor(CatalogHandle catalogHandle, ConnectorName connectorName)
    {
        return new Key(accountContext().accountId(), "SYSTEM_" + connectorName, catalogHandle.getType(), ImmutableMap.of());
    }

    private AccountContext accountContext()
    {
        return accountContext.orElseThrow(() -> new IllegalStateException("accountContext is not set"));
    }

    private void cleanCache(long maxElapsedNanos)
    {
        cache.forEach((key, entry) -> {
            CatalogConnector catalogConnectorToShutdown = null;

            synchronized (entry) {
                if (entry.useCount == 0) {
                    long elapsedNanos = Ticker.systemTicker().read() - entry.lastUse;
                    if (elapsedNanos > maxElapsedNanos) {
                        catalogConnectorToShutdown = entry.catalogConnector;
                        entry.deleted = true;
                        entry.catalogConnector = null;

                        cache.remove(key);
                    }
                }
            }

            if (catalogConnectorToShutdown != null) {
                log.debug("Cleaning cached connection. Key: %s", key);
                shutdownConnector(catalogConnectorToShutdown);
            }
        });
    }

    private void shutdownConnector(CatalogConnector catalogConnector)
    {
        try {
            catalogConnector.shutdown();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector %s (catalog %s)", catalogConnector.getConnectorName(), catalogConnector.getCatalogHandle().getCatalogName());
        }
    }
}
