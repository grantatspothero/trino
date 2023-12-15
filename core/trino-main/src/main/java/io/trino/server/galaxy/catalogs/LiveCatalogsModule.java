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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.starburst.stargate.catalog.CatalogVersionConfigurationApi;
import io.starburst.stargate.catalog.HttpCatalogVersionConfigurationClient;
import io.starburst.stargate.crypto.KmsCryptoModule;
import io.starburst.stargate.crypto.MasterKeyCrypto;
import io.starburst.stargate.crypto.SecretSealer;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogPruneTask;
import io.trino.connector.CatalogPruneTaskConfig;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.DefaultCatalogFactory;
import io.trino.connector.LazyCatalogFactory;
import io.trino.connector.WorkerDynamicCatalogManager;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.CatalogManager;
import io.trino.server.ServerConfig;
import io.trino.transaction.ForTransactionManager;
import io.trino.transaction.NoOpTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerConfig;
import jakarta.annotation.PreDestroy;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.starburst.stargate.crypto.TestingMasterKeyCrypto.createTrinoCrypto;
import static io.starburst.stargate.crypto.TestingMasterKeyCrypto.createVerifierCrypto;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LiveCatalogsModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);
        bindHttpClient(binder);
        if (serverConfig.isCoordinator()) {
            LiveCatalogsConfig liveCatalogsConfig = buildConfigObject(LiveCatalogsConfig.class);
            configBinder(binder).bindConfig(LiveCatalogsConfig.class);
            super.install(new CatalogVersionConfigurationApiProviderModule());
            if (!liveCatalogsConfig.isQueryRunnerTesting()) {
                binder.bind(SecretSealer.class).in(SINGLETON);
                if (liveCatalogsConfig.getUseKmsCrypto()) {
                    install(new KmsCryptoModule());
                }
                else {
                    switch (liveCatalogsConfig.getDeploymentType()) {
                        case DEFAULT, WARP_SPEED -> {
                            binder.bind(MasterKeyCrypto.class).toInstance(createTrinoCrypto(liveCatalogsConfig.getTrinoPlaneId().toString()));
                        }
                        case METADATA -> {
                            binder.bind(MasterKeyCrypto.class).toInstance(createVerifierCrypto(liveCatalogsConfig.getTrinoPlaneId().toString()));
                        }
                    }
                }
                binder.bind(GalaxyCatalogInfoSupplier.class).to(EncryptedSecretsGalaxyCatalogInfoSupplier.class);
                switch (liveCatalogsConfig.getDeploymentType()) {
                    case DEFAULT, WARP_SPEED -> {
                        binder.bind(DecryptionContextProvider.class).to(TrinoDecryptionContextProvider.class).in(SINGLETON);
                    }
                    case METADATA -> {
                        //TODO before Live Catalog use in Metadata service, consider transition to service plane and new decryption at the same time
                        binder.bind(DecryptionContextProvider.class).to(MetadataDecryptionContextProvider.class).in(SINGLETON);
                    }
                }
                binder.bind(Clock.class).annotatedWith(ForTransactionManager.class).toInstance(Clock.systemUTC());
            }
            //Transaction manager
            configBinder(binder).bindConfig(TransactionManagerConfig.class);
            binder.bind(LiveCatalogsTransactionManager.class).in(SINGLETON);
            binder.bind(ConnectorServicesProvider.class).to(LiveCatalogsTransactionManager.class);
            binder.bind(CatalogManager.class).to(LiveCatalogsTransactionManager.class);
            binder.bind(TransactionManager.class).to(LiveCatalogsTransactionManager.class);

            //singletons
            binder.bind(ExecutorCleanup.class).asEagerSingleton();
            binder.bind(CoordinatorLazyRegister.class).asEagerSingleton();
            configBinder(binder).bindConfig(CatalogPruneTaskConfig.class);
            binder.bind(CatalogPruneTask.class).asEagerSingleton();
        }
        else {
            binder.bind(WorkerDynamicCatalogManager.class).in(Scopes.SINGLETON);
            binder.bind(ConnectorServicesProvider.class).to(WorkerDynamicCatalogManager.class).in(Scopes.SINGLETON);
            // catalog manager is not registered on worker
            binder.bind(WorkerLazyRegister.class).asEagerSingleton();
            binder.bind(TransactionManager.class).to(NoOpTransactionManager.class).in(Scopes.SINGLETON);
        }
    }

    public static void bindHttpClient(Binder binder)
    {
        httpClientBinder(binder).bindHttpClient("galaxy-catalog-version-configuration", ForCatalogConfiguration.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(5, SECONDS));
                });
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ScheduledExecutorService createTransactionIdleCheckExecutor()
    {
        return newSingleThreadScheduledExecutor(daemonThreadsNamed("transaction-idle-check"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ExecutorService createTransactionFinishingExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("transaction-finishing-%s"));
    }

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForTransactionManager ExecutorService transactionFinishingExecutor,
                @ForTransactionManager ScheduledExecutorService transactionIdleExecutor)
        {
            executors = ImmutableList.<ExecutorService>builder()
                    .add(transactionFinishingExecutor)
                    .add(transactionIdleExecutor)
                    .build();
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }

    private static class CatalogVersionConfigurationApiProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        public static CatalogVersionConfigurationApi createCatalogVersionConfiguration(@ForCatalogConfiguration HttpClient httpClient, LiveCatalogsConfig liveCatalogsConfig, NodeVersion nodeVersion)
        {
            return new HttpCatalogVersionConfigurationClient(httpClient, liveCatalogsConfig.getCatalogConfigurationURI(), nodeVersion.getVersion());
        }
    }

    private static class CoordinatorLazyRegister
    {
        @Inject
        public CoordinatorLazyRegister(
                GlobalSystemConnector globalSystemConnector,
                LiveCatalogsTransactionManager liveCatalogsTransactionManager,
                DefaultCatalogFactory defaultCatalogFactory,
                LazyCatalogFactory lazyCatalogFactory)
        {
            lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory);
            liveCatalogsTransactionManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }

    private static class WorkerLazyRegister
    {
        @Inject
        public WorkerLazyRegister(
                DefaultCatalogFactory defaultCatalogFactory,
                LazyCatalogFactory lazyCatalogFactory,
                WorkerDynamicCatalogManager catalogManager,
                GlobalSystemConnector globalSystemConnector)
        {
            lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory);
            catalogManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }
}
