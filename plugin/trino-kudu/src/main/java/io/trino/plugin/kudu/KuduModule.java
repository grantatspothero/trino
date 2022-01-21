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
package io.trino.plugin.kudu;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.authentication.CachingKerberosAuthentication;
import io.trino.plugin.base.authentication.KerberosAuthentication;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.kudu.procedures.RangePartitionProcedures;
import io.trino.plugin.kudu.properties.KuduTableProperties;
import io.trino.plugin.kudu.schema.NoSchemaEmulation;
import io.trino.plugin.kudu.schema.SchemaEmulation;
import io.trino.plugin.kudu.schema.SchemaEmulationByTableNameConvention;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.TypeManager;
import org.apache.kudu.client.KuduClient;

import javax.inject.Singleton;
import javax.security.auth.Subject;

import java.security.PrivilegedAction;
import java.util.function.Function;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.kudu.KuduAuthenticationConfig.KuduAuthenticationType.KERBEROS;
import static io.trino.plugin.kudu.KuduAuthenticationConfig.KuduAuthenticationType.NONE;
import static io.trino.plugin.kudu.SystemProperties.setJavaSecurityKrb5Conf;
import static java.util.Objects.requireNonNull;

public class KuduModule
        extends AbstractConfigurationAwareModule
{
    private final TypeManager typeManager;

    public KuduModule(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(KuduConnector.class).in(Scopes.SINGLETON);
        binder.bind(KuduMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KuduTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(KuduSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(KuduPageSourceProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(KuduPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(KuduHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(KuduSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).annotatedWith(ForClassLoaderSafe.class).to(KuduNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(ClassLoaderSafeNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(KuduRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(KuduClientConfig.class);
        configBinder(binder).bindConfig(KuduAuthenticationConfig.class);

        install(conditionalModule(
                KuduAuthenticationConfig.class,
                authenticationConfig -> authenticationConfig.getAuthenticationType() == NONE,
                new NoneAuthenticationModule()));

        install(conditionalModule(
                KuduAuthenticationConfig.class,
                authenticationConfig -> authenticationConfig.getAuthenticationType() == KERBEROS,
                new KerberosAuthenticationModule()));

        binder.bind(RangePartitionProcedures.class).in(Scopes.SINGLETON);
        Multibinder.newSetBinder(binder, Procedure.class);
    }

    @ProvidesIntoSet
    Procedure getAddRangePartitionProcedure(RangePartitionProcedures procedures)
    {
        return procedures.getAddPartitionProcedure();
    }

    @ProvidesIntoSet
    Procedure getDropRangePartitionProcedure(RangePartitionProcedures procedures)
    {
        return procedures.getDropPartitionProcedure();
    }

    private static class NoneAuthenticationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
        }

        @Provides
        @Singleton
        public static KuduClientSession createKuduClientSession(KuduClientConfig config)
        {
            return KuduModule.createKuduClientSession(config,
                    (KuduClient.KuduClientBuilder builder) -> new DelegatingKuduClient(builder.build()));
        }
    }

    private static class KerberosAuthenticationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            configBinder(binder).bindConfig(KuduKerberosConfig.class);
        }

        @Provides
        @Singleton
        public static KuduClientSession createKuduClientSession(KuduClientConfig config, KuduKerberosConfig kuduKerberosConfig)
        {
            return KuduModule.createKuduClientSession(config,
                    (builder) -> getKerberizedKuduClient(builder, kuduKerberosConfig));
        }
    }

    private static KuduClientSession createKuduClientSession(KuduClientConfig config, Function<KuduClient.KuduClientBuilder, IKuduClient> kuduClientFactory)
    {
        requireNonNull(config, "config is null");

        KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder(config.getMasterAddresses());
        builder.defaultAdminOperationTimeoutMs(config.getDefaultAdminOperationTimeout().toMillis());
        builder.defaultOperationTimeoutMs(config.getDefaultOperationTimeout().toMillis());
        if (config.isDisableStatistics()) {
            builder.disableStatistics();
        }
        IKuduClient client = kuduClientFactory.apply(builder);

        SchemaEmulation strategy;
        if (config.isSchemaEmulationEnabled()) {
            strategy = new SchemaEmulationByTableNameConvention(config.getSchemaEmulationPrefix());
        }
        else {
            strategy = new NoSchemaEmulation();
        }
        return new KuduClientSession(client, strategy);
    }

    private static IKuduClient getKerberizedKuduClient(KuduClient.KuduClientBuilder builder, KuduKerberosConfig kuduKerberosConfig)
    {
        kuduKerberosConfig.getKuduPrincipalPrimary().ifPresent(builder::saslProtocolName);
        setJavaSecurityKrb5Conf(kuduKerberosConfig.getConfig().getAbsolutePath());
        KerberosAuthentication kerberosAuthentication = new KerberosAuthentication(kuduKerberosConfig.getClientPrincipal(), kuduKerberosConfig.getClientKeytab());
        CachingKerberosAuthentication cachingKerberosAuthentication = new CachingKerberosAuthentication(kerberosAuthentication);

        Subject subject = cachingKerberosAuthentication.getSubject();
        return Subject.doAs(subject, (PrivilegedAction<IKuduClient>) (() ->
                new KerberizedKuduClient(builder.build(), cachingKerberosAuthentication)));
    }
}
