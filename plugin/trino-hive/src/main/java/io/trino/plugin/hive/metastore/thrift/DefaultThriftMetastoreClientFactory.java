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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient.TransportSupplier;
import io.trino.spi.NodeManager;
import io.trino.sshtunnel.SshTunnelConfig;
import io.trino.sshtunnel.SshTunnelManager;
import io.trino.sshtunnel.SshTunnelProperties;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.BasicHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.util.Timeout;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DefaultThriftMetastoreClientFactory
        implements ThriftMetastoreClientFactory
{
    private final Optional<SshTunnelProperties> sshTunnelProperties;
    private final Optional<SSLContext> sslContext;
    private final Optional<HostAndPort> socksProxy;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;
    private final String hostname;

    private final MetastoreSupportsDateStatistics metastoreSupportsDateStatistics = new MetastoreSupportsDateStatistics();
    private final AtomicInteger chosenGetTableAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenTableParamAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenGetAllViewsPerDatabaseAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenAlterTransactionalTableAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenAlterPartitionsAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenGetAllTablesAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenGetAllViewsAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final Optional<ThriftHttpContext> thriftHttpContext;
    private final OpenTelemetry openTelemetry;

    public DefaultThriftMetastoreClientFactory(
            SshTunnelConfig sshTunnelConfig,
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            Duration connectTimeout,
            Duration readTimeout,
            HiveMetastoreAuthentication metastoreAuthentication,
            String hostname,
            Optional<ThriftHttpContext> thriftHttpContext,
            OpenTelemetry openTelemetry)
    {
        this.sshTunnelProperties = SshTunnelProperties.generateFrom(sshTunnelConfig);
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
        this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
        this.connectTimeoutMillis = toIntExact(connectTimeout.toMillis());
        this.readTimeoutMillis = toIntExact(readTimeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
        this.hostname = requireNonNull(hostname, "hostname is null");
        this.thriftHttpContext = requireNonNull(thriftHttpContext, "thriftHttpContext is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
    }

    @Inject
    public DefaultThriftMetastoreClientFactory(
            SshTunnelConfig sshTunnelConfig,
            ThriftMetastoreConfig config,
            ThriftHttpMetastoreConfig httpMetastoreConfig,
            HiveMetastoreAuthentication metastoreAuthentication,
            NodeManager nodeManager,
            OpenTelemetry openTelemetry)
    {
        this(
                sshTunnelConfig,
                buildSslContext(
                        config.isTlsEnabled(),
                        Optional.ofNullable(config.getKeystorePath()),
                        Optional.ofNullable(config.getKeystorePassword()),
                        Optional.ofNullable(config.getTruststorePath()),
                        Optional.ofNullable(config.getTruststorePassword())),
                Optional.ofNullable(config.getSocksProxy()),
                config.getConnectTimeout(),
                config.getReadTimeout(),
                metastoreAuthentication,
                nodeManager.getCurrentNode().getHost(),
                buildThriftHttpContext(httpMetastoreConfig),
                openTelemetry);
    }

    @Override
    public ThriftMetastoreClient create(URI uri, Optional<String> delegationToken)
            throws TTransportException
    {
        return create(() -> getTransportSupplier(uri, delegationToken), hostname);
    }

    private TTransport getTransportSupplier(URI uri, Optional<String> delegationToken)
            throws TTransportException
    {
        HostAndPort address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        HostAndPort useAddress = sshTunnelProperties.map(SshTunnelManager::getCached)
                .map(sshTunnelManager -> sshTunnelManager.getOrCreateTunnel(address))
                .map(tunnel -> HostAndPort.fromParts("localhost", tunnel.getLocalTunnelPort()))
                .orElse(address);
        switch (uri.getScheme().toLowerCase(ENGLISH)) {
            case "thrift" -> {
                return createTransport(useAddress, delegationToken);
            }
            case "http", "https" -> {
                try {
                    return createHttpTransport(
                            new URI(uri.getScheme(), uri.getUserInfo(), useAddress.getHost(), useAddress.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment()),
                            thriftHttpContext.orElseThrow(() -> new IllegalArgumentException("Thrift http context is not set")));
                }
                catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
            default -> throw new IllegalArgumentException("Invalid metastore uri scheme " + uri.getScheme());
        }
    }

    private TTransport createHttpTransport(URI uri, ThriftHttpContext httpThriftContext)
            throws TTransportException
    {
        HttpClientBuilder clientBuilder = createHttpClientBuilder(httpThriftContext);
        ConnectionConfig.custom().setConnectTimeout(Timeout.ofMilliseconds(connectTimeoutMillis)).build();
        THttpClient httpClient = new THttpClient(uri.toString(), clientBuilder.build());
        httpClient.setConnectTimeout(connectTimeoutMillis);
        return httpClient;
    }

    private HttpClientBuilder createHttpClientBuilder(ThriftHttpContext httpThriftContext)
    {
        // TODO: Hook this http client into the opentelemetry field
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        if (sslContext.isPresent()) {
            SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(sslContext.get(), new DefaultHostnameVerifier(null));
            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", socketFactory)
                    .build();
            clientBuilder.setConnectionManager(new BasicHttpClientConnectionManager(registry));
        }
        clientBuilder.addRequestInterceptorFirst((httpRequest, entityDetails, httpContext) -> {
            httpRequest.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + httpThriftContext.token());
            httpThriftContext.additionalHeaders().forEach(httpRequest::addHeader);
        });
        return clientBuilder;
    }

    protected ThriftMetastoreClient create(TransportSupplier transportSupplier, String hostname)
            throws TTransportException
    {
        return new ThriftHiveMetastoreClient(
                transportSupplier,
                hostname,
                metastoreSupportsDateStatistics,
                chosenGetTableAlternative,
                chosenTableParamAlternative,
                chosenGetAllTablesAlternative,
                chosenGetAllViewsPerDatabaseAlternative,
                chosenGetAllViewsAlternative,
                chosenAlterTransactionalTableAlternative,
                chosenAlterPartitionsAlternative);
    }

    private TTransport createTransport(HostAndPort address, Optional<String> delegationToken)
            throws TTransportException
    {
        return Transport.create(address, sslContext, socksProxy, connectTimeoutMillis, readTimeoutMillis, metastoreAuthentication, delegationToken);
    }

    private static Optional<SSLContext> buildSslContext(
            boolean tlsEnabled,
            Optional<File> keyStorePath,
            Optional<String> keyStorePassword,
            Optional<File> trustStorePath,
            Optional<String> trustStorePassword)
    {
        if (!tlsEnabled) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    record ThriftHttpContext(String token, Map<String, String> additionalHeaders)
    {
        ThriftHttpContext {
            requireNonNull(token, "token is null");
            requireNonNull(additionalHeaders, "additionalHeaders is null");
            additionalHeaders = ImmutableMap.copyOf(additionalHeaders);
        }
    }

    @VisibleForTesting
    public static Optional<ThriftHttpContext> buildThriftHttpContext(ThriftHttpMetastoreConfig config)
    {
        if (config.getHttpBearerToken() == null) {
            checkArgument(config.getAdditionalHeaders().isEmpty(), "Additional headers must be empty");
            return Optional.empty();
        }
        return Optional.of(new ThriftHttpContext(
                config.getHttpBearerToken(),
                config.getAdditionalHeaders()));
    }
}
