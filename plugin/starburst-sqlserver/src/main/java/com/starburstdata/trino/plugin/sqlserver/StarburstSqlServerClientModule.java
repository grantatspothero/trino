/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.plugin.base.galaxy.RegionVerifierProperties;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.ForJdbcDynamicFiltering;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.plugin.jdbc.ptf.Procedure;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.plugin.sqlserver.SqlServerConnectionFactory;
import io.trino.plugin.sqlserver.SqlServerSessionProperties;
import io.trino.plugin.sqlserver.SqlServerTableProperties;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Properties;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.trino.plugin.sqlserver.CatalogOverridingModule.ForCatalogOverriding;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static io.trino.plugin.sqlserver.SqlServerClient.SQL_SERVER_MAX_LIST_EXPRESSIONS;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class StarburstSqlServerClientModule
        extends AbstractConfigurationAwareModule
{
    private final LicenseManager licenseManager;

    public StarburstSqlServerClientModule(LicenseManager licenseManager)
    {
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class)).setBinding().to(SqlServerSplitManager.class).in(SINGLETON);

        configBinder(binder).bindConfig(SqlServerConfig.class);
        binder.bind(LicenseManager.class).toInstance(licenseManager);

        install(new ExtraCredentialsBasedIdentityCacheMappingModule());
        install(conditionalModule(
                StarburstSqlServerConfig.class,
                StarburstSqlServerConfig::getDatabasePrefixForSchemaEnabled,
                internalBinder -> internalBinder.bind(JdbcClient.class)
                        .annotatedWith(ForBaseJdbc.class)
                        .to(StarburstSqlServerMultiDatabaseClient.class)
                        .in(SINGLETON),
                internalBinder -> internalBinder.bind(JdbcClient.class)
                        .annotatedWith(ForBaseJdbc.class)
                        .to(StarburstSqlServerClient.class)
                        .in(SINGLETON)));

        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(SQL_SERVER_MAX_LIST_EXPRESSIONS);

        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        bindSessionPropertiesProvider(binder, SqlServerSessionProperties.class);
        bindSessionPropertiesProvider(binder, StarburstSqlServerSessionProperties.class);

        bindTablePropertiesProvider(binder, SqlServerTableProperties.class);

        install(new CredentialProviderModule());
        install(new CatalogOverridingModule());
        install(new JdbcJoinPushdownSupportModule());

        @SuppressWarnings("TrinoExperimentalSpi")
        Class<ConnectorTableFunction> clazz = ConnectorTableFunction.class;
        newSetBinder(binder, clazz).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
        install(conditionalModule(
                SqlServerConfig.class,
                SqlServerConfig::isStoredProcedureTableFunctionEnabled,
                internalBinder -> newSetBinder(internalBinder, clazz).addBinding().toProvider(Procedure.class).in(Scopes.SINGLETON)));

        // Using optional binder for overriding ConnectionFactory in Galaxy
        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForCatalogOverriding.class))
                .setDefault()
                .to(Key.get(ConnectionFactory.class, DefaultSqlserverBinding.class))
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @DefaultSqlserverBinding
    public static ConnectionFactory getConnectionFactory(
            CatalogHandle catalogHandle,
            BaseJdbcConfig config,
            SqlServerConfig sqlServerConfig,
            LocalRegionConfig localRegionConfig,
            CrossRegionConfig crossRegionConfig,
            SshTunnelConfig sshTunnelConfig,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry)
    {
        Properties socketArgs = new Properties();
        RegionVerifierProperties.addRegionVerifierProperties(socketArgs::setProperty, RegionVerifierProperties.generateFrom(localRegionConfig, crossRegionConfig));
        SshTunnelProperties.generateFrom(sshTunnelConfig)
                .ifPresent(sshTunnelProperties -> addSshTunnelProperties(socketArgs::setProperty, sshTunnelProperties));

        CatalogNetworkMonitorProperties.addCatalogNetworkMonitorProperties(socketArgs::setProperty, CatalogNetworkMonitorProperties.generateFrom(crossRegionConfig, catalogHandle));

        Properties connectionProperties = new Properties();
        connectionProperties.put("socketFactoryClass", GalaxySqlServerSocketFactory.class.getName());

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            socketArgs.store(outputStream, null);
            // serialize ssh tunnel and region enforcement properties
            connectionProperties.put("socketFactoryConstructorArg", outputStream.toString(UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not construct SocketFactory argument", e);
        }

        DriverConnectionFactory delegate = new DriverConnectionFactory(new SQLServerDriver(), config.getConnectionUrl(), connectionProperties, credentialProvider, openTelemetry);
        return new SqlServerConnectionFactory(delegate, sqlServerConfig.isSnapshotIsolationDisabled());
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @BindingAnnotation
    public @interface DefaultSqlserverBinding {}
}