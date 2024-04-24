/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.json.JsonBinder;
import io.airlift.tracing.SpanSerialization;
import io.opentelemetry.api.trace.Span;
import io.starburst.stargate.buffer.data.execution.ChunkManager;
import io.starburst.stargate.buffer.data.execution.ChunkManager.ForChunkManager;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.execution.SpooledChunksByExchange;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;
import io.starburst.stargate.buffer.data.memory.MemoryAllocatorConfig;
import io.starburst.stargate.buffer.data.spooling.MergedFileNameGenerator;
import io.starburst.stargate.buffer.status.StatusProvider;

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class MainModule
        extends AbstractConfigurationAwareModule
{
    private final long bufferNodeId;
    private final boolean discoveryBroadcastEnabled;
    private final Ticker ticker;

    public MainModule()
    {
        this(new SecureRandom().nextLong());
    }

    private MainModule(long bufferNodeId)
    {
        this(bufferNodeId, true, Ticker.systemTicker());
    }

    @VisibleForTesting
    public MainModule(long bufferNodeId, boolean discoveryBroadcastEnabled, Ticker ticker)
    {
        this.bufferNodeId = bufferNodeId;
        this.discoveryBroadcastEnabled = discoveryBroadcastEnabled;
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        httpClientBinder(binder).bindHttpClient("buffer-discovery.http", ForBufferDiscoveryClient.class);

        JsonBinder.jsonBinder(binder).addDeserializerBinding(Span.class).to(SpanSerialization.SpanDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(Span.class);

        configBinder(binder).bindConfig(ChunkManagerConfig.class);
        configBinder(binder).bindConfig(MemoryAllocatorConfig.class);
        configBinder(binder).bindConfig(DataServerConfig.class);
        jaxrsBinder(binder).bind(DataResource.class);
        jaxrsBinder(binder).bind(LifecycleResource.class);
        binder.bind(MemoryAllocator.class).in(SINGLETON);
        binder.bind(BufferNodeId.class).toInstance(new BufferNodeId(bufferNodeId));
        binder.bind(BufferNodeInfoService.class).in(SINGLETON);
        binder.bind(Ticker.class).annotatedWith(ForChunkManager.class).toInstance(ticker);
        binder.bind(ChunkManager.class).in(SINGLETON);
        binder.bind(MergedFileNameGenerator.class).in(SINGLETON);
        binder.bind(SpooledChunksByExchange.class).in(SINGLETON);
        binder.bind(DataServerStats.class).in(SINGLETON);
        newExporter(binder).export(DataServerStats.class).withGeneratedName();
        binder.bind(AddDataPagesThrottlingCalculator.class).in(SINGLETON);
        binder.bind(BufferNodeStateManager.class).in(SINGLETON);
        binder.bind(DataServerStatusProvider.class).in(SINGLETON);
        binder.bind(DrainService.class).in(SINGLETON);
        newSetBinder(binder, StatusProvider.class).addBinding().to(DataServerStatusProvider.class);
        binder.bind(ExecutorService.class).toInstance(newCachedThreadPool(daemonThreadsNamed("buffer-node-execution-%s")));
        newOptionalBinder(binder, DiscoveryBroadcast.class);
        if (discoveryBroadcastEnabled) {
            binder.bind(DiscoveryBroadcast.class).in(SINGLETON);
        }

        install(conditionalModule(
                DataServerConfig.class,
                DataServerConfig::isTestingEnableStatsLogging,
                innerBinder -> innerBinder.bind(DataServerStatsLogger.class).in(SINGLETON)));
    }

    @Provides
    @Singleton
    @ForAsyncHttp
    public static BoundedExecutor createAsyncHttpResponseExecutor(DataServerConfig config)
    {
        return new BoundedExecutor(newCachedThreadPool(daemonThreadsNamed("async-http-response-%s")), config.getHttpResponseThreads());
    }
}
