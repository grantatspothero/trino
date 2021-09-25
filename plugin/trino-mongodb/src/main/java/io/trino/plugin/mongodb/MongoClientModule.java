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
package io.trino.plugin.mongodb;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.Stream;
import com.mongodb.connection.StreamFactory;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.internal.connection.SocketStream;
import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;
import io.trino.plugin.base.galaxy.RegionEnforcementConfig;
import io.trino.plugin.mongodb.ptf.Query;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.type.TypeManager;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelProperties;

import javax.inject.Singleton;
import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogId;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCatalogName;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addCrossRegionAllowed;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addRegionLocalIpAddresses;
import static io.trino.plugin.base.galaxy.GalaxySqlSocketFactory.addTlsEnabled;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.sshtunnel.SshTunnelPropertiesMapper.addSshTunnelProperties;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MongoClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(SshTunnelConfig.class);
        configBinder(binder).bindConfig(RegionEnforcementConfig.class);

        binder.bind(MongoConnector.class).in(Scopes.SINGLETON);
        binder.bind(MongoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSinkProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(MongoClientConfig.class);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    public static MongoSession createMongoSession(
            CatalogHandle catalogHandle,
            TypeManager typeManager,
            MongoClientConfig config,
            SshTunnelConfig sshConfig,
            RegionEnforcementConfig regionEnforcementConfig)
    {
        MongoClientSettings.Builder options = MongoClientSettings.builder();
        options.writeConcern(config.getWriteConcern().getWriteConcern())
                .readPreference(config.getReadPreference().getReadPreference())
                .applyToConnectionPoolSettings(builder -> builder
                        .maxConnectionIdleTime(config.getMaxConnectionIdleTime(), MILLISECONDS)
                        .maxWaitTime(config.getMaxWaitTime(), MILLISECONDS)
                        .minSize(config.getMinConnectionsPerHost())
                        .maxSize(config.getConnectionsPerHost()))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(config.getConnectionTimeout(), MILLISECONDS)
                        .readTimeout(config.getSocketTimeout(), MILLISECONDS));

        if (config.getRequiredReplicaSetName() != null) {
            options.applyToClusterSettings(builder ->
                    builder.requiredReplicaSetName(config.getRequiredReplicaSetName()));
        }
        if (config.getTlsEnabled() == null) {
            // TODO: unconditionally apply sslEnabled/tlsEnabled (i.e. remove null check) see: https://github.com/starburstdata/stargate/issues/4474
        }
        else if (config.getTlsEnabled()) {
            options.applyToSslSettings(builder -> {
                builder.enabled(true);
                buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTruststorePath(), config.getTruststorePassword())
                        .ifPresent(builder::context);
            });
        }

        options.applyConnectionString(new ConnectionString(config.getConnectionUrl()));

        Optional<SshTunnelProperties> sshProperties = SshTunnelProperties.generateFrom(sshConfig);

        options.streamFactoryFactory((SocketSettings socketSettings, SslSettings sslSettings) ->
                new GalaxyStreamFactory(socketSettings, sslSettings, regionEnforcementConfig, sshProperties, catalogHandle));

        MongoClient client = MongoClients.create(options.build());

        return new MongoSession(
                typeManager,
                client,
                config);
    }

    // TODO https://github.com/trinodb/trino/issues/15247 Add test for x.509 certificates
    private static Optional<SSLContext> buildSslContext(
            Optional<File> keystorePath,
            Optional<String> keystorePassword,
            Optional<File> truststorePath,
            Optional<String> truststorePassword)
    {
        if (keystorePath.isEmpty() && truststorePath.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keystorePath, keystorePassword, truststorePath, truststorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private static class GalaxyStreamFactory
            implements StreamFactory
    {
        private final SocketSettings settings;
        private final SslSettings sslSettings;
        private final BufferProvider bufferProvider = new PowerOfTwoBufferPool();
        private final RegionEnforcementConfig regionEnforcementConfig;
        private final Optional<SshTunnelProperties> sshTunnelProperties;
        private final CatalogHandle catalogHandle;

        public GalaxyStreamFactory(
                SocketSettings settings,
                SslSettings sslSettings,
                RegionEnforcementConfig regionEnforcementConfig,
                Optional<SshTunnelProperties> sshTunnelProperties,
                CatalogHandle catalogHandle)
        {
            this.settings = requireNonNull(settings, "settings is null");
            this.sslSettings = requireNonNull(sslSettings, "sslSettings is null");
            this.regionEnforcementConfig = requireNonNull(regionEnforcementConfig, "regionEnforcementConfig is null");
            this.sshTunnelProperties = requireNonNull(sshTunnelProperties, "sshTunnelProperties is null");
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        }

        @Override
        public Stream create(ServerAddress serverAddress)
        {
            if (serverAddress instanceof UnixServerAddress) {
                throw new MongoClientException("Galaxy only supports IP sockets, not unix domain sockets");
            }
            Properties properties = new Properties();

            addCatalogName(properties, catalogHandle.getCatalogName());
            addCatalogId(properties, catalogHandle.getVersion().toString());
            addCrossRegionAllowed(properties, false);
            addRegionLocalIpAddresses(properties, regionEnforcementConfig.getAllowedIpAddresses());
            sshTunnelProperties.ifPresent(sshProps -> addSshTunnelProperties(properties::setProperty, sshProps));
            if (sslSettings.isEnabled()) {
                addTlsEnabled(properties);
            }

            return new SocketStream(serverAddress, settings, sslSettings, new GalaxySqlSocketFactory(properties), bufferProvider);
        }
    }
}
