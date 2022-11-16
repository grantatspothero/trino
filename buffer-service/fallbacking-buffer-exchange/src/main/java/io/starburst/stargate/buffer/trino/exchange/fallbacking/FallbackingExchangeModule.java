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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.starburst.stargate.buffer.discovery.client.failures.FailureTrackingApi;
import io.starburst.stargate.buffer.discovery.client.failures.HttpFailureTrackingClient;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeManager;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManager;

import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class FallbackingExchangeModule
        extends AbstractConfigurationAwareModule
{
    private final BufferExchangeManager bufferExchangeManager;
    private final FileSystemExchangeManager filesystemExchangeManager;

    public FallbackingExchangeModule(BufferExchangeManager bufferExchangeManager, FileSystemExchangeManager filesystemExchangeManager)
    {
        this.bufferExchangeManager = requireNonNull(bufferExchangeManager, "bufferExchangeManager is null");
        this.filesystemExchangeManager = requireNonNull(filesystemExchangeManager, "filesystemExchangeManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(BufferExchangeManager.class).toInstance(bufferExchangeManager);
        binder.bind(FileSystemExchangeManager.class).toInstance(filesystemExchangeManager);

        binder.bind(ScheduledExecutorService.class).toInstance(newSingleThreadScheduledExecutor(threadsNamed("fallbacking-exchange")));

        configBinder(binder).bindConfig(FallbackingExchangeConfig.class);
        httpClientBinder(binder).bindHttpClient("exchange.failure-tracking.http", ForFailureTrackingClient.class);
        binder.bind(ExceptionAnalyzer.class).in(Scopes.SINGLETON);
        binder.bind(FallbackSelector.class).in(Scopes.SINGLETON);

        binder.bind(FallbackingExchangeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FallbackingExchangeStats.class).withGeneratedName();

        binder.bind(FallbackingExchangeManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    public FailureTrackingApi getFailureTrackingApi(FallbackingExchangeConfig config, @ForFailureTrackingClient HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        requireNonNull(httpClient, "httpClient is null");
        return new HttpFailureTrackingClient(config.getFailureTrackingServiceUri(), httpClient);
    }
}
