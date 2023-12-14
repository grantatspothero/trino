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
package io.trino.server.galaxy.catalogs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.BiMap;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.CatalogVersion;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.NotInTransactionException;
import io.trino.Session;
import io.trino.connector.CatalogConnector;
import io.trino.connector.CatalogFactory;
import io.trino.connector.CatalogProperties;
import io.trino.connector.ConnectorName;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.CatalogMetadata;
import io.trino.plugin.base.galaxy.GalaxyCatalogVersionUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.transaction.ForTransactionManager;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionInfo;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerConfig;
import org.joda.time.DateTime;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.trino.plugin.base.galaxy.GalaxyCatalogVersionUtils.fromCatalogIdAndLiveCatalogUniqueId;
import static io.trino.plugin.base.galaxy.GalaxyCatalogVersionUtils.getRequiredLiveCatalogUniqueId;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_KILLED;
import static io.trino.spi.StandardErrorCode.AUTOCOMMIT_WRITE_CONFLICT;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.MULTI_CATALOG_WRITE_CONFLICT;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static io.trino.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * This class is designed for multi-tenant use and separates account resources.
 */
public class LiveCatalogsTransactionManager
        implements TransactionManager, ConnectorServicesProvider, CatalogManager, CatalogResolver
{
    private static final Logger log = Logger.get(LiveCatalogsTransactionManager.class);

    private final ConcurrentHashMap<TransactionId, TransactionMetadata> transactions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<AccountId, ConcurrentHashMap<CatalogVersion, UUID>> accountToCatalogVersionId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, GalaxyLiveCatalog> galaxyLiveCatalogs = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock modifyGalaxyLiveCatalogsLock = new ReentrantReadWriteLock();
    private final AtomicReference<CatalogConnector> globalSystemCatalogConnector = new AtomicReference<>();
    private final GalaxyCatalogInfoSupplier galaxyCatalogInfoSupplier;
    private final CatalogFactory catalogFactory;
    private final int maxFinishingConcurrency;
    private final Executor finishingExecutor;
    private final Duration idleTimeout;
    private final Duration maxCatalogStaleness;
    private final Duration maxOldVersionStaleness;
    private final Clock clock;

    @Inject
    public LiveCatalogsTransactionManager(
            TransactionManagerConfig transactionManagerConfig,
            LiveCatalogsConfig liveCatalogsConfig,
            GalaxyCatalogInfoSupplier galaxyCatalogInfoSupplier,
            CatalogFactory catalogFactory,
            @ForTransactionManager ScheduledExecutorService idleCheckExecutor,
            @ForTransactionManager ExecutorService finishingExecutor,
            @ForTransactionManager Clock clock)
    {
        this.galaxyCatalogInfoSupplier = requireNonNull(galaxyCatalogInfoSupplier, "galaxyCatalogInfoSupplier is null");
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.maxFinishingConcurrency = transactionManagerConfig.getMaxFinishingConcurrency();
        this.finishingExecutor = requireNonNull(finishingExecutor, "finishingExecutor is null");
        this.idleTimeout = requireNonNull(transactionManagerConfig.getIdleTimeout(), "idleTimeout is null");
        this.maxCatalogStaleness = requireNonNull(liveCatalogsConfig.getMaxCatalogStaleness());
        this.maxOldVersionStaleness = requireNonNull(liveCatalogsConfig.getOldVersionStaleness());
        this.clock = requireNonNull(clock, "clock is null");
        scheduleIdleChecks(transactionManagerConfig.getIdleCheckInterval(), idleCheckExecutor);
    }

    public void setCatalogsForTransaction(DispatchSession dispatchSession, TransactionId transactionId, List<CatalogVersion> catalogs)
    {
        ReadLock readLock = modifyGalaxyLiveCatalogsLock.readLock();
        readLock.lock();
        try {
            ImmutableList.Builder<GalaxyLiveCatalog> galaxyLiveCatalogsForTransaction = ImmutableList.builder();
            for (CatalogVersion catalogVersion : catalogs) {
                // We can parallelize this block to lower overall fetching latency if it proves to be an actual problem
                GalaxyCatalogArgs galaxyCatalogArgs = new GalaxyCatalogArgs(
                        dispatchSession.accountId(),
                        catalogVersion);
                UUID liveCatalogId = accountToCatalogVersionId
                        .computeIfAbsent(dispatchSession.accountId(), ignore -> new ConcurrentHashMap<>())
                        .computeIfAbsent(catalogVersion, ignore -> UUID.randomUUID());
                GalaxyLiveCatalog galaxyLiveCatalog = this.galaxyLiveCatalogs.computeIfAbsent(liveCatalogId, ignore -> new GalaxyLiveCatalog(
                        liveCatalogId,
                        galaxyCatalogArgs,
                        galaxyCatalogInfoSupplier.getGalaxyCatalogInfo(
                                dispatchSession,
                                galaxyCatalogArgs),
                        catalogFactory,
                        clock));
                verify(galaxyLiveCatalog.getCatalogConstructionArgs().equals(galaxyCatalogArgs), "Uuid collision");
                galaxyLiveCatalogsForTransaction.add(galaxyLiveCatalog);
            }
            transactions.get(transactionId).setGalaxyLiveCatalogs(galaxyLiveCatalogsForTransaction.build());
        }
        finally {
            readLock.unlock();
        }
    }

    @VisibleForTesting
    public Duration getMaxOldVersionStaleness()
    {
        return maxOldVersionStaleness;
    }

    @VisibleForTesting
    public Duration getMaxCatalogStaleness()
    {
        return maxCatalogStaleness;
    }

    @VisibleForTesting
    public boolean contains(GalaxyCatalogArgs galaxyCatalogArgs)
    {
        return accountToCatalogVersionId.containsKey(galaxyCatalogArgs.accountId()) && accountToCatalogVersionId.get(galaxyCatalogArgs.accountId()).containsKey(galaxyCatalogArgs.catalogVersion());
    }

    @VisibleForTesting
    public synchronized void cleanUpStaleCatalogs()
    {
        ImmutableMap.Builder<UUID, Duration> cleanupTargets = ImmutableMap.builder();
        // if max staleness set, collect stale catalogs of every type
        Instant cleanUpInstant = clock.instant();
        galaxyLiveCatalogs.forEach((liveCatalogId, galaxyLiveCatalog) -> {
            if (galaxyLiveCatalog.lastTransactionEndedMoreThan(maxCatalogStaleness, cleanUpInstant)) {
                cleanupTargets.put(liveCatalogId, maxCatalogStaleness);
            }
        });

        // collect versions of the catalog that aren't the max version that haven't been used in the retention period
        accountToCatalogVersionId.values()
                .forEach(accountCatalogVersions -> {
                    Map<CatalogId, PriorityQueue<Map.Entry<CatalogVersion, UUID>>> catalogVersionOrder = new HashMap<>();
                    accountCatalogVersions.entrySet().forEach(catalogVersionUuidEntry -> {
                        CatalogVersion catalogVersion = catalogVersionUuidEntry.getKey();
                        catalogVersionOrder.computeIfAbsent(catalogVersion.catalogId(), ignore ->
                                        new PriorityQueue<>(Comparator.comparingLong(versionAndIdEntry -> versionAndIdEntry.getKey().version().getVersion())))
                                .add(catalogVersionUuidEntry);
                    });
                    catalogVersionOrder.values().forEach(versions -> {
                        while (versions.size() > 1) {
                            UUID oldVersionGalaxyLiveCatalogId = versions.poll().getValue();
                            if (galaxyLiveCatalogs.get(oldVersionGalaxyLiveCatalogId).lastTransactionEndedMoreThan(maxOldVersionStaleness, cleanUpInstant)) {
                                cleanupTargets.put(oldVersionGalaxyLiveCatalogId, maxOldVersionStaleness);
                            }
                        }
                    });
                });

        // let old version retention duration overwrite staleness duration
        // built on assuming old versions should have lower retention than the catalog's latest version
        Map<UUID, Duration> cleanUpTargetWithExpiration = cleanupTargets.buildKeepingLast();
        ImmutableList.Builder<GalaxyLiveCatalog> removedGalaxyLiveCatalogs = ImmutableList.builder();
        if (!cleanUpTargetWithExpiration.isEmpty()) {
            WriteLock writeLock = modifyGalaxyLiveCatalogsLock.writeLock();
            writeLock.lock();
            try {
                Set<UUID> idsInTransaction = transactions.values().stream().map(TransactionMetadata::getGalaxyLiveCatalogIds).flatMap(Set::stream).collect(toImmutableSet());
                cleanUpTargetWithExpiration
                        .forEach((liveCatalogId, expirationDuration) -> {
                            // if the UUID is not currently in use by a transaction or last use is longer ago than its specific expiration. Remove it.
                            if (!idsInTransaction.contains(liveCatalogId)
                                    && galaxyLiveCatalogs.get(liveCatalogId).lastTransactionEndedMoreThan(expirationDuration, cleanUpInstant)) {
                                GalaxyCatalogArgs galaxyCatalogArgs = galaxyLiveCatalogs.get(liveCatalogId).getCatalogConstructionArgs();
                                Map<CatalogVersion, UUID> accountVersion = accountToCatalogVersionId.get(galaxyCatalogArgs.accountId());
                                accountVersion.remove(galaxyCatalogArgs.catalogVersion());
                                if (accountVersion.isEmpty()) {
                                    accountToCatalogVersionId.remove(galaxyCatalogArgs.accountId());
                                }
                                removedGalaxyLiveCatalogs.add(galaxyLiveCatalogs.remove(liveCatalogId));
                            }
                        });
            }
            finally {
                writeLock.unlock();
                for (GalaxyLiveCatalog removedGalaxyLiveCatalog : removedGalaxyLiveCatalogs.build()) {
                    finishingExecutor.execute(() -> cleanUpGalaxyLiveCatalog(removedGalaxyLiveCatalog));
                }
            }
        }
    }

    private void cleanUpGalaxyLiveCatalog(GalaxyLiveCatalog toCleanup)
    {
        try {
            toCleanup.shutdown();
        }
        catch (Throwable t) {
            log.error(t, "Error cleaning up catalog connector");
        }
    }

    @Override
    public boolean isReadOnlyCatalog(Optional<TransactionId> requiredTransactionId, String catalogName)
    {
        return getTransactionMetadata(requiredTransactionId.orElseThrow(() -> new NullPointerException("Required Transaction Id not found")))
                .getCatalogHandle(catalogName)
                .map(CatalogHandle::getVersion)
                .map(GalaxyCatalogVersionUtils::getRequiredLiveCatalogUniqueId)
                .map(galaxyLiveCatalogs::get)
                .map(GalaxyLiveCatalog::getReadOnly)
                .orElseThrow(() -> new TrinoException(CATALOG_NOT_FOUND, "Catalog '%s' does not exist".formatted(catalogName)));
    }

    @Override
    public Optional<CatalogId> getCatalogId(Optional<TransactionId> requiredTransactionId, String catalogName)
    {
        return Optional.ofNullable(getTransactionMetadata(requiredTransactionId.orElseThrow(() -> new VerifyException("Required Transaction Id not found")))
                .getCatalogNamesToCatalogIds().get(catalogName));
    }

    @Override
    public Optional<String> getCatalogName(Optional<TransactionId> requiredTransactionId, CatalogId catalogId)
    {
        return Optional.ofNullable(getTransactionMetadata(requiredTransactionId.orElseThrow(() -> new VerifyException("Required Transaction Id not found")))
                .getCatalogNamesToCatalogIds().inverse().get(catalogId));
    }

    @Override
    public Optional<SharedSchemaNameAndAccepted> getSharedSchemaForCatalog(Optional<TransactionId> requiredTransactionId, String catalogName)
    {
        return getTransactionMetadata(requiredTransactionId.orElseThrow(() -> new VerifyException("Required Transaction Id not found")))
                .getCatalogHandle(catalogName)
                .map(CatalogHandle::getVersion)
                .map(GalaxyCatalogVersionUtils::getRequiredLiveCatalogUniqueId)
                .map(galaxyLiveCatalogs::get)
                .flatMap(GalaxyLiveCatalog::getSharedSchema);
    }

    @Override
    public void loadInitialCatalogs()
    {
        // no-op
    }

    @Override
    public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs)
    {
        // happens ad-hoc
    }

    @Override
    public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
    {
        // happens in background
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        if (catalogHandle.getRootCatalogHandle().equals(GlobalSystemConnector.CATALOG_HANDLE)) {
            return globalSystemCatalogConnector.get().getMaterializedConnector(catalogHandle.getType());
        }
        UUID liveCatalogId = getRequiredLiveCatalogUniqueId(catalogHandle.getVersion());
        return Optional.ofNullable(galaxyLiveCatalogs.get(liveCatalogId))
                .map(GalaxyLiveCatalog::getCatalogConnector)
                .orElseThrow(() -> new VerifyException("Cannot find Galaxy ConnectorService for handle"))
                .getMaterializedConnector(catalogHandle.getType());
    }

    public void registerGlobalSystemConnector(GlobalSystemConnector connector)
    {
        if (!globalSystemCatalogConnector.compareAndSet(null, catalogFactory.createCatalog(GlobalSystemConnector.CATALOG_HANDLE, new ConnectorName(GlobalSystemConnector.NAME), connector))) {
            throw new VerifyException("Global system connector should be set once");
        }
    }

    @Override
    public Set<String> getCatalogNames()
    {
        // usage in addConnectorEventListeners which is a system that is not supported for Galaxy/Galaxy Plugins
        // Tagged with "TODO: remove connector event listeners or add support for dynamic loading from connector"
        // Also used in updateConnectorIds which is tagged with "TODO: remove this huge hack"
        // other usages are testing or internal
        throw new UnsupportedOperationException("Cannot get all catalog names in live catalog mode");
    }

    @Override
    public Optional<Catalog> getCatalog(String catalogName)
    {
        // usage in addConnectorEventListeners which is a system that is not supported for Galaxy/Galaxy Plugins
        // Tagged with "TODO: remove connector event listeners or add support for dynamic loading from connector"
        // Also used in updateConnectorIds which is tagged with "TODO: remove this huge hack"
        // other usages are testing or internal
        throw new UnsupportedOperationException("Cannot get catalog by name in live catalog mode");
    }

    @Override
    public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
    {
        if (catalogHandle.getRootCatalogHandle().equals(GlobalSystemConnector.CATALOG_HANDLE)) {
            // global system catalog not distributed to workers
            return Optional.empty();
        }
        UUID liveCatalogId = getRequiredLiveCatalogUniqueId(catalogHandle.getVersion());
        return Optional.ofNullable(galaxyLiveCatalogs.get(liveCatalogId)).map(GalaxyLiveCatalog::getCatalogProperties);
    }

    @Override
    public Set<CatalogHandle> getActiveCatalogs()
    {
        return galaxyLiveCatalogs.values().stream()
                .map(GalaxyLiveCatalog::getCatalogProperties)
                .map(CatalogProperties::getCatalogHandle)
                .collect(toImmutableSet());
    }

    @Override
    public void createCatalog(String catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
    {
        throw new TrinoException(NOT_SUPPORTED, "CREATE CATALOG is not supported by Galaxy");
    }

    @Override
    public void dropCatalog(String catalogName, boolean exists)
    {
        throw new TrinoException(NOT_SUPPORTED, "DROP CATALOG is not supported by Galaxy");
    }

    private void scheduleIdleChecks(Duration idleCheckInterval, ScheduledExecutorService idleCheckExecutor)
    {
        idleCheckExecutor.scheduleWithFixedDelay(() -> {
            try {
                cleanUpExpiredTransactions();
                cleanUpStaleCatalogs();
            }
            catch (Throwable t) {
                log.error(t, "Unexpected exception while cleaning up expired transactions and catalogs");
            }
        }, idleCheckInterval.toMillis(), idleCheckInterval.toMillis(), MILLISECONDS);
    }

    private synchronized void cleanUpExpiredTransactions()
    {
        Iterator<Map.Entry<TransactionId, TransactionMetadata>> iterator = transactions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TransactionId, TransactionMetadata> entry = iterator.next();
            if (entry.getValue().isExpired(idleTimeout)) {
                iterator.remove();
                log.info("Removing expired transaction: %s", entry.getKey());
                entry.getValue().asyncAbort();
            }
        }
    }

    @Override
    public boolean transactionExists(TransactionId transactionId)
    {
        return transactions.containsKey(transactionId);
    }

    @Override
    public TransactionInfo getTransactionInfo(TransactionId transactionId)
    {
        return getTransactionMetadata(transactionId).getTransactionInfo().orElseThrow(() -> new TrinoException(TRANSACTION_CONFLICT, "Transaction not initialized"));
    }

    @Override
    public Optional<TransactionInfo> getTransactionInfoIfExist(TransactionId transactionId)
    {
        return tryGetTransactionMetadata(transactionId).flatMap(TransactionMetadata::getTransactionInfo);
    }

    @Override
    public List<TransactionInfo> getAllTransactionInfos()
    {
        return transactions.values().stream()
                .map(TransactionMetadata::getTransactionInfo)
                .flatMap(Optional::stream)
                .collect(toImmutableList());
    }

    @Override
    public Set<TransactionId> getTransactionsUsingCatalog(CatalogHandle catalogHandle)
    {
        return transactions.values().stream()
                .filter(transactionMetadata -> transactionMetadata.isUsingCatalog(catalogHandle))
                .map(TransactionMetadata::getTransactionId)
                .collect(toImmutableSet());
    }

    @Override
    public TransactionId beginTransaction(boolean autoCommitContext)
    {
        return beginTransaction(DEFAULT_ISOLATION, DEFAULT_READ_ONLY, autoCommitContext);
    }

    @Override
    public TransactionId beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext)
    {
        TransactionId transactionId = TransactionId.create();
        BoundedExecutor executor = new BoundedExecutor(finishingExecutor, maxFinishingConcurrency);
        TransactionMetadata transactionMetadata = new TransactionMetadata(transactionId, isolationLevel, readOnly, autoCommitContext, executor, globalSystemCatalogConnector.get());
        checkState(transactions.put(transactionId, transactionMetadata) == null, "Duplicate transaction ID: %s", transactionId);
        return transactionId;
    }

    @Override
    public List<CatalogInfo> getCatalogs(TransactionId transactionId)
    {
        return getTransactionMetadata(transactionId).listCatalogs();
    }

    @Override
    public List<CatalogInfo> getActiveCatalogs(TransactionId transactionId)
    {
        return getTransactionMetadata(transactionId).getActiveCatalogs();
    }

    @Override
    public Optional<CatalogHandle> getCatalogHandle(TransactionId transactionId, String catalogName)
    {
        return getTransactionMetadata(transactionId).getCatalogHandle(catalogName);
    }

    @Override
    public Optional<CatalogMetadata> getOptionalCatalogMetadata(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);
        return transactionMetadata.getCatalogHandle(catalogName).map(transactionMetadata::getTransactionCatalogMetadata);
    }

    @Override
    public CatalogMetadata getCatalogMetadata(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        return getTransactionMetadata(transactionId).getTransactionCatalogMetadata(catalogHandle);
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadata(transactionId, catalogHandle);
        checkConnectorWrite(transactionId, catalogHandle);
        return catalogMetadata;
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);

        CatalogHandle catalogHandle = transactionMetadata.getCatalogHandle(catalogName)
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Catalog does not exist: " + catalogName));

        return getCatalogMetadataForWrite(transactionId, catalogHandle);
    }

    @Override
    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);

        CatalogHandle catalogHandle = transactionMetadata.getCatalogHandle(catalogName)
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Catalog does not exist: " + catalogName));

        return transactionMetadata.getTransactionCatalogMetadata(catalogHandle).getTransactionHandleFor(catalogHandle);
    }

    @Override
    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        return getCatalogMetadata(transactionId, catalogHandle).getTransactionHandleFor(catalogHandle);
    }

    private void checkConnectorWrite(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        getTransactionMetadata(transactionId).checkConnectorWrite(catalogHandle);
    }

    @Override
    public void checkAndSetActive(TransactionId transactionId)
    {
        TransactionMetadata metadata = getTransactionMetadata(transactionId);
        metadata.checkOpenTransaction();
        metadata.setActive();
    }

    @Override
    public void trySetActive(TransactionId transactionId)
    {
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::setActive);
    }

    @Override
    public void trySetInactive(TransactionId transactionId)
    {
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::setInactive);
    }

    @Override
    public ListenableFuture<Void> asyncCommit(TransactionId transactionId)
    {
        return nonCancellationPropagating(Futures.transformAsync(removeTransactionMetadataAsFuture(transactionId), TransactionMetadata::asyncCommit, directExecutor()));
    }

    @Override
    public ListenableFuture<Void> asyncAbort(TransactionId transactionId)
    {
        return nonCancellationPropagating(Futures.transformAsync(removeTransactionMetadataAsFuture(transactionId), TransactionMetadata::asyncAbort, directExecutor()));
    }

    @Override
    public void blockCommit(TransactionId transactionId, String reason)
    {
        getTransactionMetadata(transactionId).blockCommit(reason);
    }

    @Override
    public void fail(TransactionId transactionId)
    {
        // Mark transaction as failed, but don't remove it.
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::asyncAbort);
    }

    private TransactionMetadata getTransactionMetadata(TransactionId transactionId)
    {
        TransactionMetadata transactionMetadata = transactions.get(transactionId);
        if (transactionMetadata == null) {
            throw new NotInTransactionException(transactionId);
        }
        return transactionMetadata;
    }

    private Optional<TransactionMetadata> tryGetTransactionMetadata(TransactionId transactionId)
    {
        return Optional.ofNullable(transactions.get(transactionId));
    }

    private ListenableFuture<TransactionMetadata> removeTransactionMetadataAsFuture(TransactionId transactionId)
    {
        ReadLock readLock = modifyGalaxyLiveCatalogsLock.readLock();
        readLock.lock();
        try {
            TransactionMetadata transactionMetadata = transactions.remove(transactionId);
            if (transactionMetadata == null) {
                return immediateFailedFuture(new NotInTransactionException(transactionId));
            }
            Instant now = clock.instant();
            transactionMetadata.getGalaxyLiveCatalogIds().forEach(id -> galaxyLiveCatalogs.get(id).transactionEnded(now));
            return immediateFuture(transactionMetadata);
        }
        finally {
            readLock.unlock();
        }
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @ThreadSafe
    private static class TransactionMetadata
    {
        private final DateTime createTime = DateTime.now();
        private final TransactionId transactionId;
        private final IsolationLevel isolationLevel;
        private final boolean readOnly;
        private final boolean autoCommitContext;
        private final Executor finishingExecutor;
        private final CatalogConnector globalSystemCatalogConnector;
        private final AtomicReference<List<GalaxyLiveCatalog>> transactionGalaxyLiveCatalogs = new AtomicReference<>();
        private final AtomicReference<String> commitBlocked = new AtomicReference<>();
        private final AtomicReference<Boolean> completedSuccessfully = new AtomicReference<>();
        private final AtomicReference<Long> idleStartTime = new AtomicReference<>();
        private final AtomicReference<CatalogHandle> writtenCatalog = new AtomicReference<>();
        @GuardedBy("this")
        private final Map<CatalogHandle, CatalogMetadata> activeCatalogs = new ConcurrentHashMap<>();
        @GuardedBy("this")
        private BiMap<String, CatalogId> catalogNamesToCatalogId;
        @GuardedBy("this")
        private Map<String, CatalogHandle> catalogNamesToHandles;
        @GuardedBy("this")
        private Map<CatalogHandle, CatalogInfo> catalogHandlesToCatalogInfo;
        @GuardedBy("this")
        private Map<UUID, GalaxyLiveCatalog> idToLiveCatalog;

        public TransactionMetadata(
                TransactionId transactionId,
                IsolationLevel isolationLevel,
                boolean readOnly,
                boolean autoCommitContext,
                Executor finishingExecutor,
                CatalogConnector globalSystemCatalogConnector)
        {
            this.transactionId = requireNonNull(transactionId, "transactionId is null");
            this.isolationLevel = requireNonNull(isolationLevel, "isolationLevel is null");
            this.readOnly = readOnly;
            this.autoCommitContext = autoCommitContext;
            this.finishingExecutor = requireNonNull(finishingExecutor, "finishingExecutor is null");
            this.globalSystemCatalogConnector = requireNonNull(globalSystemCatalogConnector, "globalSystemCatalogConnectorSupplier is null");
        }

        public synchronized void setGalaxyLiveCatalogs(List<GalaxyLiveCatalog> galaxyLiveCatalogs)
        {
            if (!this.transactionGalaxyLiveCatalogs.compareAndSet(null, ImmutableList.copyOf(galaxyLiveCatalogs))) {
                throw new TrinoException(TRANSACTION_CONFLICT, "Catalogs already associated with transaction");
            }

            catalogNamesToCatalogId = galaxyLiveCatalogs.stream()
                    .collect(toImmutableBiMap(
                            galaxyLiveCatalog -> galaxyLiveCatalog.getCatalogProperties().getCatalogHandle().getCatalogName(),
                            galaxyLiveCatalog -> galaxyLiveCatalog.getCatalogConstructionArgs().catalogVersion().catalogId()));

            catalogNamesToHandles = ImmutableMap.<String, CatalogHandle>builder()
                    .put(GlobalSystemConnector.NAME, GlobalSystemConnector.CATALOG_HANDLE)
                    .putAll(galaxyLiveCatalogs.stream()
                            .map(GalaxyLiveCatalog::getCatalogProperties)
                            .collect(toImmutableMap(
                                    properties -> properties.getCatalogHandle().getCatalogName(),
                                    CatalogProperties::getCatalogHandle)))
                    .buildOrThrow();

            catalogHandlesToCatalogInfo = ImmutableMap.<CatalogHandle, CatalogInfo>builder()
                    .put(GlobalSystemConnector.CATALOG_HANDLE, new CatalogInfo(GlobalSystemConnector.NAME, GlobalSystemConnector.CATALOG_HANDLE, new ConnectorName(GlobalSystemConnector.NAME)))
                    .putAll(galaxyLiveCatalogs.stream()
                            .map(GalaxyLiveCatalog::getCatalogProperties)
                            .collect(toImmutableMap(
                                    CatalogProperties::getCatalogHandle,
                                    properties -> new CatalogInfo(properties.getCatalogHandle().getCatalogName(), properties.getCatalogHandle(), properties.getConnectorName()))))
                    .buildOrThrow();

            idToLiveCatalog = galaxyLiveCatalogs.stream()
                    .collect(toImmutableMap(
                            GalaxyLiveCatalog::getId,
                            Function.identity()));
        }

        private void requireCatalogsSet()
        {
            if (transactionGalaxyLiveCatalogs.get() == null) {
                throw new IllegalStateException("Catalogs must be set before using transaction");
            }
        }

        public TransactionId getTransactionId()
        {
            return transactionId;
        }

        public Set<UUID> getGalaxyLiveCatalogIds()
        {
            List<GalaxyLiveCatalog> liveCatalogs = transactionGalaxyLiveCatalogs.get();
            if (liveCatalogs == null) {
                // This method can be called before Transaction Initialized with catalogs
                return ImmutableSet.of();
            }
            return liveCatalogs.stream().map(GalaxyLiveCatalog::getId).collect(toImmutableSet());
        }

        public synchronized BiMap<String, CatalogId> getCatalogNamesToCatalogIds()
        {
            requireCatalogsSet();
            return catalogNamesToCatalogId;
        }

        public synchronized Optional<TransactionInfo> getTransactionInfo()
        {
            if (transactionGalaxyLiveCatalogs.get() == null) {
                // This method can be called before Transaction Initialized with catalogs
                return Optional.empty();
            }
            Duration idleTime = Optional.ofNullable(idleStartTime.get())
                    .map(Duration::nanosSince)
                    .orElse(new Duration(0, MILLISECONDS));

            // catalog handle catalog names are assumed correct for our use case
            Optional<String> writtenCatalogName = Optional.ofNullable(this.writtenCatalog.get()).map(CatalogHandle::getCatalogName);

            List<String> catalogNames = catalogNamesToHandles
                    .keySet().stream()
                    .sorted()
                    .collect(toImmutableList());

            return Optional.of(new TransactionInfo(
                    transactionId,
                    isolationLevel,
                    readOnly,
                    autoCommitContext,
                    createTime,
                    idleTime,
                    catalogNames,
                    writtenCatalogName,
                    ImmutableSet.copyOf(catalogNamesToHandles.values())));
        }

        public synchronized boolean isUsingCatalog(CatalogHandle catalogHandle)
        {
            if (transactionGalaxyLiveCatalogs.get() == null) {
                // This method can be called before Transaction Initialized with catalogs
                return false;
            }
            return catalogHandlesToCatalogInfo.containsKey(catalogHandle);
        }

        public synchronized List<CatalogInfo> getActiveCatalogs()
        {
            requireCatalogsSet();
            return activeCatalogs.keySet()
                    .stream()
                    .map(catalogHandlesToCatalogInfo::get)
                    .filter(Objects::nonNull)
                    .collect(toImmutableList());
        }

        public synchronized List<CatalogInfo> listCatalogs()
        {
            return ImmutableList.copyOf(catalogHandlesToCatalogInfo.values());
        }

        public synchronized Optional<CatalogHandle> getCatalogHandle(String catalogName)
        {
            requireCatalogsSet();
            return Optional.ofNullable(catalogNamesToHandles.get(catalogName));
        }

        private synchronized CatalogMetadata getTransactionCatalogMetadata(CatalogHandle catalogHandle)
        {
            requireCatalogsSet();
            checkOpenTransaction();
            CatalogMetadata catalogMetadata = activeCatalogs.get(catalogHandle.getRootCatalogHandle());
            if (catalogMetadata == null) {
                // catalog name will not be an internal catalog (e.g., information schema) because internal
                // catalog references can only be generated from the main catalog
                checkArgument(!catalogHandle.getType().isInternal(), "Internal catalog handle not allowed: %s", catalogHandle);
                CatalogConnector catalogConnector;
                if (catalogHandle.equals(GlobalSystemConnector.CATALOG_HANDLE)) {
                    catalogConnector = globalSystemCatalogConnector;
                }
                else {
                    UUID liveCatalogId = getRequiredLiveCatalogUniqueId(catalogHandle.getVersion());
                    verify(idToLiveCatalog.containsKey(liveCatalogId), "catalog not part of transaction");
                    catalogConnector = idToLiveCatalog.get(liveCatalogId).getCatalogConnector();
                }
                catalogMetadata = catalogConnector.getCatalog().beginTransaction(transactionId, isolationLevel, readOnly, autoCommitContext);

                activeCatalogs.put(catalogHandle, catalogMetadata);
            }
            return catalogMetadata;
        }

        public synchronized void checkConnectorWrite(CatalogHandle catalogHandle)
        {
            checkOpenTransaction();
            CatalogMetadata catalogMetadata = activeCatalogs.get(catalogHandle);
            checkArgument(catalogMetadata != null, "Cannot record write for catalog not part of transaction");
            if (readOnly) {
                throw new TrinoException(READ_ONLY_VIOLATION, "Cannot execute write in a read-only transaction");
            }
            if (!writtenCatalog.compareAndSet(null, catalogHandle) && !writtenCatalog.get().equals(catalogHandle)) {
                String writtenCatalogName = activeCatalogs.get(writtenCatalog.get()).getCatalogName();
                throw new TrinoException(MULTI_CATALOG_WRITE_CONFLICT, "Multi-catalog writes not supported in a single transaction. Already wrote to catalog " + writtenCatalogName);
            }
            if (catalogMetadata.isSingleStatementWritesOnly() && !autoCommitContext) {
                throw new TrinoException(AUTOCOMMIT_WRITE_CONFLICT, "Catalog only supports writes using autocommit: " + catalogMetadata.getCatalogName());
            }
        }

        public synchronized ListenableFuture<Void> asyncCommit()
        {
            if (!completedSuccessfully.compareAndSet(null, true)) {
                if (completedSuccessfully.get()) {
                    // Already done
                    return immediateVoidFuture();
                }
                // Transaction already aborted
                return immediateFailedFuture(new TrinoException(TRANSACTION_ALREADY_ABORTED, "Current transaction has already been aborted"));
            }

            String commitBlockedReason = commitBlocked.get();
            if (commitBlockedReason != null) {
                return Futures.transform(
                        abortInternal(),
                        ignored -> {
                            throw new TrinoException(ADMINISTRATIVELY_KILLED, commitBlockedReason);
                        },
                        directExecutor());
            }

            CatalogHandle writeCatalogHandle = this.writtenCatalog.get();
            if (writeCatalogHandle == null) {
                ListenableFuture<Void> future = asVoid(Futures.allAsList(activeCatalogs.values().stream()
                        .map(catalog -> Futures.submit(catalog::commit, finishingExecutor))
                        .collect(toList())));
                addExceptionCallback(future, throwable -> {
                    abortInternal();
                    log.error(throwable, "Read-only connector should not throw exception on commit");
                });
                return nonCancellationPropagating(future);
            }

            Supplier<ListenableFuture<Void>> commitReadOnlyConnectors = () -> {
                List<ListenableFuture<Void>> futures = activeCatalogs.entrySet().stream()
                        .filter(entry -> !entry.getKey().equals(writeCatalogHandle))
                        .map(Map.Entry::getValue)
                        .map(transactionMetadata -> Futures.submit(transactionMetadata::commit, finishingExecutor))
                        .collect(toList());
                ListenableFuture<Void> future = asVoid(Futures.allAsList(futures));
                addExceptionCallback(future, throwable -> log.error(throwable, "Read-only connector should not throw exception on commit"));
                return future;
            };

            CatalogMetadata writeCatalog = activeCatalogs.get(writeCatalogHandle);
            ListenableFuture<Void> commitFuture = Futures.submit(writeCatalog::commit, finishingExecutor);
            ListenableFuture<Void> readOnlyCommitFuture = Futures.transformAsync(commitFuture, ignored -> commitReadOnlyConnectors.get(), directExecutor());
            addExceptionCallback(readOnlyCommitFuture, this::abortInternal);
            return nonCancellationPropagating(readOnlyCommitFuture);
        }

        public synchronized ListenableFuture<Void> asyncAbort()
        {
            if (!completedSuccessfully.compareAndSet(null, false)) {
                if (completedSuccessfully.get()) {
                    // Should not happen normally
                    return immediateFailedFuture(new IllegalStateException("Current transaction already committed"));
                }
                // Already done
                return immediateVoidFuture();
            }
            return abortInternal();
        }

        private synchronized ListenableFuture<Void> abortInternal()
        {
            // the callbacks in statement performed on another thread so are safe
            List<ListenableFuture<Void>> futures = activeCatalogs.values().stream()
                    .map(catalog -> Futures.submit(catalog::abort, finishingExecutor))
                    .collect(toList());
            ListenableFuture<Void> future = asVoid(Futures.allAsList(futures));
            return nonCancellationPropagating(future);
        }

        public void setActive()
        {
            idleStartTime.set(null);
        }

        public void setInactive()
        {
            idleStartTime.set(System.nanoTime());
        }

        public boolean isExpired(Duration idleTimeout)
        {
            Long idleStartTime = this.idleStartTime.get();
            return idleStartTime != null && Duration.nanosSince(idleStartTime).compareTo(idleTimeout) > 0;
        }

        public void blockCommit(String reason)
        {
            commitBlocked.set(requireNonNull(reason, "reason is null"));
        }

        public void checkOpenTransaction()
        {
            Boolean completedStatus = this.completedSuccessfully.get();
            if (completedStatus != null) {
                if (completedStatus) {
                    // Should not happen normally
                    throw new IllegalStateException("Current transaction already committed");
                }
                throw new TrinoException(TRANSACTION_ALREADY_ABORTED, "Current transaction is aborted, commands ignored until end of transaction block");
            }
        }
    }

    @ThreadSafe
    private static class GalaxyLiveCatalog
    {
        private final UUID id;
        private final GalaxyCatalogArgs galaxyCatalogArgs;
        private final CatalogFactory catalogFactory;
        private final boolean readOnly;
        private final CatalogProperties catalogProperties;
        private final AtomicReference<Instant> lastTransactionEnd = new AtomicReference<>();
        private final Optional<SharedSchemaNameAndAccepted> sharedSchema;

        private CatalogConnector catalogConnector;

        public GalaxyLiveCatalog(
                UUID id,
                GalaxyCatalogArgs galaxyCatalogArgs,
                GalaxyCatalogInfo galaxyCatalogInfo,
                CatalogFactory catalogFactory,
                Clock clock)
        {
            this.id = requireNonNull(id, "id is null");
            this.galaxyCatalogArgs = requireNonNull(galaxyCatalogArgs, "catalogConstructionArgs is null");
            requireNonNull(galaxyCatalogInfo, "galaxyCatalogInfo is null");
            CatalogProperties propertiesWithWrongVersion = galaxyCatalogInfo.catalogProperties();
            catalogProperties = new CatalogProperties(
                    propertiesWithWrongVersion.getCatalogHandle().withVersion(
                            fromCatalogIdAndLiveCatalogUniqueId(galaxyCatalogArgs.catalogVersion().catalogId(), id)),
                    propertiesWithWrongVersion.getConnectorName(),
                    propertiesWithWrongVersion.getProperties());
            readOnly = galaxyCatalogInfo.readOnly();
            this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
            this.sharedSchema = galaxyCatalogInfo.sharedSchema();
            lastTransactionEnd.set(clock.instant());
        }

        public UUID getId()
        {
            return id;
        }

        public GalaxyCatalogArgs getCatalogConstructionArgs()
        {
            return galaxyCatalogArgs;
        }

        public CatalogProperties getCatalogProperties()
        {
            return catalogProperties;
        }

        public boolean getReadOnly()
        {
            return readOnly;
        }

        public Optional<SharedSchemaNameAndAccepted> getSharedSchema()
        {
            return sharedSchema;
        }

        public CatalogConnector getCatalogConnector()
        {
            if (catalogConnector != null) {
                return catalogConnector;
            }

            synchronized (this) {
                if (catalogConnector == null) {
                    catalogConnector = catalogFactory.createCatalog(catalogProperties);
                }
                return catalogConnector;
            }
        }

        public synchronized void shutdown()
        {
            if (catalogConnector != null) {
                catalogConnector.shutdown();
                catalogConnector = null;
            }
        }

        public void transactionEnded(Instant transactionEnd)
        {
            lastTransactionEnd.updateAndGet(current -> Comparators.max(current, transactionEnd));
        }

        boolean lastTransactionEndedMoreThan(Duration ago, Instant fromNow)
        {
            if (fromNow.toEpochMilli() - lastTransactionEnd.get().toEpochMilli() < 0) {
                return false;
            }
            return new Duration(fromNow.toEpochMilli() - lastTransactionEnd.get().toEpochMilli(), MILLISECONDS).compareTo(ago) > 0;
        }
    }
}
