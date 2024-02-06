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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.starburst.stargate.accesscontrol.client.HttpTrinoSecurityClient;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.catalog.QueryCatalog;
import io.starburst.stargate.id.AccountId;
import io.trino.NotInTransactionException;
import io.trino.Session;
import io.trino.connector.CatalogConnector;
import io.trino.connector.CatalogProperties;
import io.trino.connector.ConnectorName;
import io.trino.connector.ConnectorServices;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.CatalogMetadata;
import io.trino.server.galaxy.GalaxyPermissionsCache;
import io.trino.server.security.galaxy.ForGalaxySystemAccessControl;
import io.trino.server.security.galaxy.GalaxyAccessControl;
import io.trino.server.security.galaxy.GalaxyAccessControlConfig;
import io.trino.server.security.galaxy.GalaxyIndexerTrinoSecurityApi;
import io.trino.server.security.galaxy.GalaxySecurityMetadata;
import io.trino.server.security.galaxy.GalaxySystemAccessControlConfig;
import io.trino.server.security.galaxy.GalaxySystemAccessController;
import io.trino.server.security.galaxy.MetadataAccessControllerSupplier;
import io.trino.server.security.galaxy.MetadataSystemSecurityMetadata;
import io.trino.server.security.galaxy.StaticCatalogResolver;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.Identity;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionInfo;
import io.trino.transaction.TransactionManager;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.INDEXER;
import static io.trino.server.security.galaxy.GalaxyIdentity.getGalaxyIdentityType;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
import static io.trino.spi.StandardErrorCode.MULTI_CATALOG_WRITE_CONFLICT;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class MetadataOnlyTransactionManager
        implements TransactionManager, ConnectorServicesProvider
{
    private static final Logger log = Logger.get(MetadataOnlyTransactionManager.class);

    private final ConcurrentMap<TransactionId, TransactionMetadata> transactions = new ConcurrentHashMap<>();
    private final CachingCatalogFactory catalogFactory;
    private final HttpClient accessControlClient;
    private final MetadataAccessControllerSupplier accessController;
    private final MetadataSystemSecurityMetadata securityMetadata;
    private final GalaxyPermissionsCache permissionsCache;
    private final Tracer tracer;
    private final AtomicReference<CatalogConnector> systemConnector = new AtomicReference<>();

    @Inject
    public MetadataOnlyTransactionManager(
            CachingCatalogFactory catalogFactory,
            @ForGalaxySystemAccessControl HttpClient accessControlClient,
            MetadataAccessControllerSupplier accessController,
            MetadataSystemSecurityMetadata securityMetadata,
            GalaxyPermissionsCache permissionsCache,
            Tracer tracer)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.accessControlClient = requireNonNull(accessControlClient, "accessControlClient is null");
        this.accessController = requireNonNull(accessController, "accessController is null");
        this.securityMetadata = requireNonNull(securityMetadata, "securityMetadata is null");
        this.permissionsCache = requireNonNull(permissionsCache, "permissionsCache is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    public void registerQueryCatalogs(
            AccountId accountId,
            Identity identity,
            TransactionId transactionId,
            QueryId queryId,
            List<QueryCatalog> catalogs,
            Map<String, String> serviceProperties,
            Span parentSpan,
            UnaryOperator<QueryCatalog> decryptProc)
    {
        CachingCatalogFactory contextCatalogFactory = catalogFactory.withAccountContext(new AccountContext(accountId, catalogs, decryptProc));
        TransactionMetadata transactionMetadata = new TransactionMetadata(
                identity,
                queryId,
                transactionId,
                catalogs,
                serviceProperties,
                contextCatalogFactory,
                systemConnector.get(),
                accessControlClient,
                accessController,
                securityMetadata,
                permissionsCache,
                tracer,
                parentSpan);
        transactions.putIfAbsent(transactionId, transactionMetadata);
    }

    public void destroyQueryCatalogs(TransactionId transactionId)
    {
        TransactionMetadata transactionMetadata = transactions.remove(transactionId);
        if (transactionMetadata != null) {
            transactionMetadata.destroy();
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
        return getTransactionMetadata(transactionId).getTransactionInfo();
    }

    @Override
    public Optional<TransactionInfo> getTransactionInfoIfExist(TransactionId transactionId)
    {
        return Optional.ofNullable(transactions.get(transactionId))
                .map(TransactionMetadata::getTransactionInfo);
    }

    @Override
    public List<CatalogInfo> getCatalogs(TransactionId transactionId)
    {
        return getTransactionMetadata(transactionId).listCatalogs();
    }

    @Override
    public List<CatalogInfo> getActiveCatalogs(TransactionId transactionId)
    {
        // this is only used for distributing catalogs, and that is not supported
        return ImmutableList.of();
    }

    @Override
    public Optional<CatalogHandle> getCatalogHandle(TransactionId transactionId, String catalogName)
    {
        return getTransactionMetadata(transactionId).tryActivateCatalog(catalogName);
    }

    @Override
    public Optional<CatalogMetadata> getOptionalCatalogMetadata(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);
        return transactionMetadata.tryActivateCatalog(catalogName)
                .map(transactionMetadata::getTransactionCatalogMetadata);
    }

    @Override
    public CatalogMetadata getCatalogMetadata(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        return getTransactionMetadata(transactionId).getTransactionCatalogMetadata(catalogHandle);
    }

    @Override
    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);

        CatalogHandle catalogHandle = transactionMetadata.tryActivateCatalog(catalogName)
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Catalog does not exist: " + catalogName));

        return transactionMetadata.getTransactionCatalogMetadata(catalogHandle).getTransactionHandleFor(catalogHandle);
    }

    @Override
    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        return getCatalogMetadata(transactionId, catalogHandle).getTransactionHandleFor(catalogHandle);
    }

    private TransactionMetadata getTransactionMetadata(TransactionId transactionId)
    {
        TransactionMetadata transactionMetadata = transactions.get(transactionId);
        if (transactionMetadata == null) {
            throw new NotInTransactionException(transactionId);
        }
        return transactionMetadata;
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        if (catalogHandle.getCatalogName().equals(GlobalSystemConnector.NAME)) {
            return systemConnector.get().getMaterializedConnector(catalogHandle.getType());
        }

        TransactionId transactionId = TransactionId.valueOf(catalogHandle.getVersion().toString());
        TransactionMetadata transactionMetadata = transactions.get(transactionId);
        if (transactionMetadata == null) {
            throw new TrinoException(CATALOG_NOT_AVAILABLE, "No catalog " + catalogHandle);
        }
        return transactionMetadata.getConnectorService(catalogHandle);
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);
        transactionMetadata.checkOpenTransaction();
        CatalogMetadata catalogMetadata = transactionMetadata.activeCatalogs.get(catalogHandle);
        if (catalogMetadata == null) {
            throw new TrinoException(CATALOG_NOT_AVAILABLE, "No catalog " + catalogHandle);
        }
        transactionMetadata.checkConnectorWrite(catalogHandle);
        return catalogMetadata;
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);
        return transactionMetadata.getCatalogHandle(catalogName)
                .map(catalogHandle -> getCatalogMetadataForWrite(transactionId, catalogHandle))
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Catalog does not exist: " + catalogName));
    }

    @Override
    public ListenableFuture<Void> asyncCommit(TransactionId transactionId)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);
        return transactionMetadata.asyncCommit();
    }

    //
    // Not implemented
    //

    @Override
    public List<TransactionInfo> getAllTransactionInfos()
    {
        return ImmutableList.of();
    }

    @Override
    public Set<TransactionId> getTransactionsUsingCatalog(CatalogHandle catalogHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "Getting transactions using catalog not supported");
    }

    @Override
    public TransactionId beginTransaction(boolean autoCommitContext)
    {
        throw new TrinoException(NOT_SUPPORTED, "Begin transaction not supported");
    }

    @Override
    public TransactionId beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext)
    {
        throw new TrinoException(NOT_SUPPORTED, "Begin transaction not supported");
    }

    @Override
    public void checkAndSetActive(TransactionId transactionId)
    {
        if (!transactions.containsKey(transactionId)) {
            throw new NotInTransactionException(transactionId);
        }
    }

    @Override
    public void trySetActive(TransactionId transactionId) {}

    @Override
    public void trySetInactive(TransactionId transactionId) {}

    @Override
    public ListenableFuture<Void> asyncAbort(TransactionId transactionId)
    {
        return immediateFuture(null);
    }

    @Override
    public void blockCommit(TransactionId transactionId, String reason) {}

    @Override
    public void fail(TransactionId transactionId) {}

    @Override
    public void loadInitialCatalogs() {}

    @Override
    public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs) {}

    public void registerGlobalSystemConnector(GlobalSystemConnector systemConnector)
    {
        CatalogConnector systemCatalogConnector = catalogFactory.createCatalog(GlobalSystemConnector.CATALOG_HANDLE, new ConnectorName(GlobalSystemConnector.NAME), systemConnector);
        this.systemConnector.set(systemCatalogConnector);
    }

    @Override
    public void pruneCatalogs(Set<CatalogHandle> catalogsInUse) {}

    @VisibleForTesting
    public boolean hasActiveTransactions()
    {
        return !transactions.isEmpty();
    }

    @ThreadSafe
    private static class TransactionMetadata
    {
        private final QueryId queryId;
        private final TransactionId transactionId;
        private final CachingCatalogFactory catalogFactory;

        private final AtomicReference<Boolean> completedSuccessfully = new AtomicReference<>();

        private final Map<String, CatalogProperties> catalogs;

        @GuardedBy("this")
        private final Map<String, CatalogConnector> connectors = new ConcurrentHashMap<>();

        @GuardedBy("this")
        private final Map<CatalogHandle, CatalogMetadata> activeCatalogs = new ConcurrentHashMap<>();
        private final TransactionInfo transactionInfo;
        private final MetadataAccessControllerSupplier galaxyMetadataAccessControl;
        private final MetadataSystemSecurityMetadata securityMetadata;
        private final Tracer tracer;
        private final Span parentSpan;
        @GuardedBy("this")
        private final AtomicReference<CatalogHandle> writtenCatalog = new AtomicReference<>();

        public TransactionMetadata(
                Identity identity,
                QueryId queryId,
                TransactionId transactionId,
                List<QueryCatalog> catalogs,
                Map<String, String> serviceProperties,
                CachingCatalogFactory catalogFactory,
                CatalogConnector systemConnector,
                HttpClient accessControlClient,
                MetadataAccessControllerSupplier galaxyMetadataAccessControl,
                MetadataSystemSecurityMetadata securityMetadata,
                GalaxyPermissionsCache permissionsCache,
                Tracer tracer,
                Span parentSpan)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.transactionId = requireNonNull(transactionId, "transactionId is null");
            this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
            this.galaxyMetadataAccessControl = requireNonNull(galaxyMetadataAccessControl, "galaxyMetadataAccessControl is null");
            this.securityMetadata = requireNonNull(securityMetadata, "securityMetadata is null");
            this.tracer = requireNonNull(tracer, "tracer is null");
            this.parentSpan = requireNonNull(parentSpan, "parentSpan is null");

            requireNonNull(catalogs, "catalogs is null");
            requireNonNull(systemConnector, "systemConnector is null");
            requireNonNull(permissionsCache, "permissionsCache is null");

            ImmutableMap.Builder<String, CatalogProperties> catalogsBuilder = ImmutableMap.builder();
            catalogsBuilder.put(systemConnector.getConnectorName().toString(), new CatalogProperties(systemConnector.getCatalogHandle(), systemConnector.getConnectorName(), ImmutableMap.of()));
            catalogs.stream()
                    .map(catalog -> toCatalogProperties(catalog, transactionId))
                    .forEach(catalog -> catalogsBuilder.put(catalog.getCatalogHandle().getCatalogName(), catalog));
            this.catalogs = catalogsBuilder.buildOrThrow();

            // pre activate the system catalog since it can't be constructed from properties
            connectors.put(systemConnector.getConnectorName().toString(), systemConnector);

            transactionInfo = new TransactionInfo(
                    transactionId,
                    READ_UNCOMMITTED,
                    true,
                    true,
                    new DateTime(),
                    new Duration(0, TimeUnit.MILLISECONDS),
                    ImmutableList.copyOf(this.catalogs.keySet()),
                    Optional.empty(),
                    // TODO activeCatalogs?
                    ImmutableSet.of());

            if (!requiredServiceProperty(serviceProperties, "access-control.name").equals(GalaxyAccessControl.NAME)) {
                throw new IllegalArgumentException("access-control.name must be %s".formatted(GalaxyAccessControl.NAME));
            }
            Optional<URI> accessControlUri = Optional.ofNullable(serviceProperties.get("galaxy.access-control-url")).map(URI::create);
            URI uri = accessControlUri.orElse(URI.create(requiredServiceProperty(serviceProperties, "galaxy.account-url")));

            TrinoSecurityApi securityClient = (getGalaxyIdentityType(identity) == INDEXER) ?
                    new GalaxyIndexerTrinoSecurityApi(
                            catalogs.stream().collect(toImmutableBiMap(QueryCatalog::catalogId, QueryCatalog::catalogName)),
                            catalogs.stream().filter(catalog -> catalog.sharedSchema().isPresent()).collect(toImmutableMap(QueryCatalog::catalogId, catalog -> catalog.sharedSchema().get())))
                    : new HttpTrinoSecurityClient(uri, accessControlClient);

            GalaxyAccessControlConfig galaxyAccessControlConfig = new GalaxyAccessControlConfig()
                    .setCatalogNames(requiredServiceProperty(serviceProperties, "galaxy.catalog-names"));
            GalaxySystemAccessControlConfig systemConfig = new GalaxySystemAccessControlConfig()
                    .setReadOnlyCatalogs(serviceProperties.getOrDefault("galaxy.read-only-catalogs", ""))
                    .setSharedCatalogSchemaNames(serviceProperties.getOrDefault("galaxy.shared-catalog-schemas", ""));
            StaticCatalogResolver catalogResolver = new StaticCatalogResolver(galaxyAccessControlConfig, systemConfig);

            galaxyMetadataAccessControl.addController(transactionId, new GalaxySystemAccessController(securityClient, catalogResolver, permissionsCache, Optional.of(transactionId)));
            securityMetadata.add(transactionId, new GalaxySecurityMetadata(securityClient, catalogResolver));
        }

        public void checkOpenTransaction()
        {
            Boolean completedStatus = this.completedSuccessfully.get();
            if (completedStatus != null) {
                if (completedStatus) {
                    // Should not happen normally
                    throw new IllegalStateException("Current transaction already committed");
                }
                else {
                    throw new TrinoException(TRANSACTION_ALREADY_ABORTED, "Current transaction is aborted, commands ignored until end of transaction block");
                }
            }
        }

        public TransactionInfo getTransactionInfo()
        {
            return transactionInfo;
        }

        public List<CatalogInfo> listCatalogs()
        {
            return catalogs.entrySet().stream()
                    .map(entry -> new CatalogInfo(entry.getKey(), entry.getValue().getCatalogHandle(), entry.getValue().getConnectorName()))
                    .collect(toImmutableList());
        }

        public synchronized Optional<CatalogHandle> tryActivateCatalog(String catalogName)
        {
            if (catalogName.equals(GlobalSystemConnector.NAME)) {
                return Optional.of(GlobalSystemConnector.CATALOG_HANDLE);
            }

            CatalogProperties catalogProperties = catalogs.get(catalogName);
            if (catalogProperties == null) {
                return Optional.empty();
            }
            CatalogConnector catalogConnector = connectors.computeIfAbsent(catalogName, name -> {
                Span span = tracer.spanBuilder("metadata-create-catalogs")
                        .setParent(Context.current().with(parentSpan))
                        .startSpan();
                try (var ignore = scopedSpan(span)) {
                    return catalogFactory.createCatalog(catalogProperties);
                }
            });
            return Optional.of(catalogConnector.getCatalogHandle());
        }

        private static String requiredServiceProperty(Map<String, String> serviceProperties, String name)
        {
            return requireNonNull(serviceProperties.get(name), "Required service property not found: %s".formatted(name));
        }

        private Optional<CatalogHandle> getCatalogHandle(String catalogName)
        {
            return activeCatalogs.keySet()
                    .stream()
                    .filter(catalogHandle -> catalogHandle.getCatalogName().equals(catalogName))
                    .findFirst();
        }

        private synchronized CatalogMetadata getTransactionCatalogMetadata(CatalogHandle catalogHandle)
        {
            checkOpenTransaction();

            CatalogMetadata catalogMetadata = activeCatalogs.get(catalogHandle.getRootCatalogHandle());
            if (catalogMetadata == null) {
                // catalog name will not be an internal catalog (e.g., information schema) because internal
                // catalog references can only be generated from the main catalog
                checkArgument(!catalogHandle.getType().isInternal(), "Internal catalog handle not allowed: %s", catalogHandle);
                Catalog catalog = Optional.ofNullable(connectors.get(catalogHandle.getCatalogName()))
                        .orElseThrow(() -> new IllegalArgumentException("No catalog registered for handle: " + catalogHandle))
                        .getCatalog();

                catalogMetadata = catalog.beginTransaction(transactionId, READ_UNCOMMITTED, true, true);

                activeCatalogs.put(catalogHandle, catalogMetadata);
            }
            return catalogMetadata;
        }

        public synchronized ConnectorServices getConnectorService(CatalogHandle catalogHandle)
        {
            CatalogConnector catalogConnector = connectors.get(catalogHandle.getCatalogName());
            if (catalogConnector == null) {
                throw new TrinoException(CATALOG_NOT_AVAILABLE, "No catalog " + catalogHandle);
            }
            return catalogConnector.getMaterializedConnector(catalogHandle.getType());
        }

        public synchronized void checkConnectorWrite(CatalogHandle catalogHandle)
        {
            checkOpenTransaction();
            CatalogMetadata catalogMetadata = activeCatalogs.get(catalogHandle);
            checkArgument(catalogMetadata != null, "Cannot record write for catalog not part of transaction");
            if (!writtenCatalog.compareAndSet(null, catalogHandle) && !writtenCatalog.get().equals(catalogHandle)) {
                String writtenCatalogName = activeCatalogs.get(writtenCatalog.get()).getCatalogName();
                throw new TrinoException(MULTI_CATALOG_WRITE_CONFLICT, "Multi-catalog writes not supported in a single transaction. Already wrote to catalog " + writtenCatalogName);
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

            CatalogHandle writeCatalogHandle = this.writtenCatalog.get();
            if (writeCatalogHandle == null) {
                ListenableFuture<Void> future = asVoid(Futures.allAsList(activeCatalogs.values().stream()
                        .map(catalog -> Futures.submit(catalog::commit, directExecutor()))
                        .collect(toList())));
                addExceptionCallback(future, throwable -> {
                    log.error(throwable, "Read-only connector should not throw exception on commit");
                });
                return nonCancellationPropagating(future);
            }

            Supplier<ListenableFuture<Void>> commitReadOnlyConnectors = () -> {
                List<ListenableFuture<Void>> futures = activeCatalogs.entrySet().stream()
                        .filter(entry -> !entry.getKey().equals(writeCatalogHandle))
                        .map(Map.Entry::getValue)
                        .map(transactionMetadata -> Futures.submit(transactionMetadata::commit, directExecutor()))
                        .collect(toList());
                ListenableFuture<Void> future = asVoid(Futures.allAsList(futures));
                addExceptionCallback(future, throwable -> log.error(throwable, "Read-only connector should not throw exception on commit"));
                return future;
            };

            CatalogMetadata writeCatalog = activeCatalogs.get(writeCatalogHandle);
            ListenableFuture<Void> commitFuture = Futures.submit(writeCatalog::commit, directExecutor());
            ListenableFuture<Void> readOnlyCommitFuture = Futures.transformAsync(commitFuture, ignored -> commitReadOnlyConnectors.get(), directExecutor());
            return nonCancellationPropagating(readOnlyCommitFuture);
        }

        public void destroy()
        {
            galaxyMetadataAccessControl.removeController(transactionId);
            securityMetadata.remove(transactionId);

            Collection<CatalogMetadata> activeCatalogs;
            Collection<CatalogConnector> connectors;
            synchronized (this) {
                activeCatalogs = this.activeCatalogs.values();
                connectors = this.connectors.values();
            }

            for (CatalogMetadata catalog : activeCatalogs) {
                try {
                    catalog.abort();
                }
                catch (Throwable e) {
                    log.error(e, "Error aborting transaction for catalog %s for query %s", catalog.getCatalogName(), queryId);
                }
            }

            connectors.forEach(catalogFactory::releaseCatalogConnector);
        }

        private CatalogProperties toCatalogProperties(QueryCatalog queryCatalog, TransactionId transactionId)
        {
            return new CatalogProperties(createRootCatalogHandle(queryCatalog.catalogName(), new CatalogVersion(transactionId.toString())), new ConnectorName(queryCatalog.connectorName()), queryCatalog.properties());
        }
    }
}
