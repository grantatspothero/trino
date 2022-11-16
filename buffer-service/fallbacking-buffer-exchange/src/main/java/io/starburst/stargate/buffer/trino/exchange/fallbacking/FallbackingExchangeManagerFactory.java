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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeManager;
import io.starburst.stargate.buffer.trino.exchange.BufferExchangeManagerFactory;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.jmx.PrefixObjectNameGeneratorModule;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManager;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManagerFactory;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class FallbackingExchangeManagerFactory
        implements ExchangeManagerFactory
{
    private static final String BUFFER_EXCHANGE_CONFIG_PREFIX = "buffer.";
    private static final String FILESYSTEM_EXCHANGE_CONFIG_PREFIX = "filesystem.";

    private final BufferExchangeManagerFactory bufferExchangeManagerFactory;
    private final FileSystemExchangeManagerFactory fileSystemExchangeManagerFactory;

    public FallbackingExchangeManagerFactory(
            BufferExchangeManagerFactory bufferExchangeManagerFactory,
            FileSystemExchangeManagerFactory fileSystemExchangeManagerFactory)
    {
        this.bufferExchangeManagerFactory = requireNonNull(bufferExchangeManagerFactory, "bufferExchangeManagerFactory is null");
        this.fileSystemExchangeManagerFactory = requireNonNull(fileSystemExchangeManagerFactory, "fileSystemExchangeManagerFactory is null");
    }

    @Override
    public String getName()
    {
        return "bufferwithfallback";
    }

    @Override
    public ExchangeManager create(Map<String, String> config)
    {
        ImmutableMap.Builder<String, String> bufferExchangeConfigBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> filesystemExchangeConfigBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> remainingConfigBuilder = ImmutableMap.builder();

        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(BUFFER_EXCHANGE_CONFIG_PREFIX)) {
                bufferExchangeConfigBuilder.put(key.substring(BUFFER_EXCHANGE_CONFIG_PREFIX.length()), value);
                continue;
            }
            if (key.startsWith(FILESYSTEM_EXCHANGE_CONFIG_PREFIX)) {
                filesystemExchangeConfigBuilder.put(key.substring(FILESYSTEM_EXCHANGE_CONFIG_PREFIX.length()), value);
                continue;
            }
            remainingConfigBuilder.put(key, value);
        }

        // It is not elegant to assume specific classes here but it simplifies the implementation and the code is temporary.
        // Also, internally in FallbackingExchange* we determine routing based on handles class anyway.
        BufferExchangeManager bufferExchangeManager = (BufferExchangeManager) bufferExchangeManagerFactory.create(bufferExchangeConfigBuilder.build());
        FileSystemExchangeManager filesystemExchangeManager = (FileSystemExchangeManager) fileSystemExchangeManagerFactory.create(filesystemExchangeConfigBuilder.build());

        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
                new MBeanModule(),
                new MBeanServerModule(),
                new PrefixObjectNameGeneratorModule("io.trino.plugin.exchange.filesystem", "trino.plugin.exchange.filesystem"),
                new FallbackingExchangeModule(bufferExchangeManager, filesystemExchangeManager));

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(remainingConfigBuilder.buildOrThrow())
                .initialize();

        return injector.getInstance(FallbackingExchangeManager.class);
    }
}
