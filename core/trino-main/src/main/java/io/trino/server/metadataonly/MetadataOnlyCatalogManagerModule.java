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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.stargate.crypto.KmsCryptoModule;
import io.starburst.stargate.crypto.MasterKeyCrypto;
import io.starburst.stargate.crypto.SecretSealer;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.DefaultCatalogFactory;
import io.trino.connector.LazyCatalogFactory;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.execution.QueryManagerConfig;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.operator.RetryPolicy;
import io.trino.server.security.galaxy.GalaxySystemAccessModule;
import io.trino.server.security.galaxy.MetadataSystemSecurityMetadata;
import io.trino.server.ui.NoWebUiAuthenticationFilter;
import io.trino.server.ui.WebUiAuthenticationFilter;
import io.trino.transaction.TransactionManager;

import javax.inject.Inject;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.starburst.stargate.crypto.TestingMasterKeyCrypto.createVerifierCrypto;
import static io.trino.connector.CatalogManagerConfig.CatalogMangerKind.METADATA_ONLY;

public class MetadataOnlyCatalogManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jaxrsBinder(binder).bind(MetadataOnlyStatementResource.class);
        jaxrsBinder(binder).bind(MetadataOnlySystemResource.class);

        binder.bind(MetadataOnlyTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(TransactionManager.class).to(MetadataOnlyTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(WebUiAuthenticationFilter.class).to(NoWebUiAuthenticationFilter.class).in(Scopes.SINGLETON);
        binder.bind(MetadataOnlyLogging.class).asEagerSingleton();
        binder.bind(MetadataOnlySystemState.class).in(Scopes.SINGLETON);
        binder.bind(CachingCatalogFactory.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorServicesProvider.class).to(MetadataOnlyTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(CatalogManager.class).to(MetadataOnlyCatalogManager.class).in(Scopes.SINGLETON);
        binder.bind(GlobalSystemConnectorLazyRegistrar.class).asEagerSingleton();

        MetadataOnlyConfig metadataOnlyConfig = buildConfigObject(MetadataOnlyConfig.class);
        if (metadataOnlyConfig.getUseKmsCrypto()) {
            install(new KmsCryptoModule());
        }
        else {
            // TODO(jlz) cc Nik - due to time constraints the Metadata specific KMS policy is not ready - for now use the verifier's
            binder.bind(MasterKeyCrypto.class).toInstance(createVerifierCrypto(metadataOnlyConfig.getTrinoPlaneId().toString()));
        }
        binder.bind(SecretSealer.class).in(SINGLETON);
        configBinder(binder).bindConfig(MetadataOnlyConfig.class);

        QueryManagerConfig queryManagerConfig = buildConfigObject(QueryManagerConfig.class);
        if (queryManagerConfig.getRetryPolicy() == RetryPolicy.TASK) {
            binder.addError("%s does not support %s", METADATA_ONLY, RetryPolicy.TASK);
        }

        install(new GalaxySystemAccessModule());
        newOptionalBinder(binder, SystemSecurityMetadata.class).setBinding().to(MetadataSystemSecurityMetadata.class).in(SINGLETON);
        binder.bind(MetadataSystemSecurityMetadata.class).in(SINGLETON);
    }

    // Dummy class to lazy register global system catalog with transaction manager which has a circular dependency
    private static class GlobalSystemConnectorLazyRegistrar
    {
        @Inject
        public GlobalSystemConnectorLazyRegistrar(
                DefaultCatalogFactory defaultCatalogFactory,
                LazyCatalogFactory lazyCatalogFactory,
                MetadataOnlyTransactionManager transactionManager,
                GlobalSystemConnector globalSystemConnector)
        {
            lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory);
            transactionManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }
}
