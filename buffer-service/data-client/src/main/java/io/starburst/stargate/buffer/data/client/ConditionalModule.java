/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import java.util.Optional;
import java.util.function.Predicate;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

// TODO update airlifts ConditionalModule and drop this one
public class ConditionalModule<T>
        extends AbstractConfigurationAwareModule
{
    /**
     * @deprecated Use {@link #conditionalModule} instead
     */
    @Deprecated
    public static <T> Module installModuleIf(Class<T> config, Predicate<T> predicate, Module module, Module otherwise)
    {
        return conditionalModule(config, predicate, module, otherwise);
    }

    /**
     * @deprecated Use {@link #conditionalModule} instead
     */
    @Deprecated
    public static <T> Module installModuleIf(Class<T> config, Predicate<T> predicate, Module module)
    {
        return conditionalModule(config, predicate, module);
    }

    public static <T> Module conditionalModule(Class<T> config, Predicate<T> predicate, Module module, Module otherwise)
    {
        return combine(
                conditionalModule(config, predicate, module),
                conditionalModule(config, predicate.negate(), otherwise));
    }

    public static <T> Module conditionalModule(Class<T> config, String prefix, Predicate<T> predicate, Module module, Module otherwise)
    {
        return combine(
                conditionalModule(config, prefix, predicate, module),
                conditionalModule(config, prefix, predicate.negate(), otherwise));
    }

    public static <T> Module conditionalModule(Class<T> config, Predicate<T> predicate, Module module)
    {
        return conditionalModule(config, Optional.empty(), predicate, module);
    }

    public static <T> Module conditionalModule(Class<T> config, String prefix, Predicate<T> predicate, Module module)
    {
        return conditionalModule(config, Optional.of(prefix), predicate, module);
    }

    public static <T> Module conditionalModule(Class<T> config, Optional<String> prefix, Predicate<T> predicate, Module module)
    {
        return new ConditionalModule<>(config, prefix, predicate, module);
    }

    private final Class<T> config;
    private final Optional<String> prefix;
    private final Predicate<T> predicate;
    private final Module module;

    private ConditionalModule(Class<T> config, Optional<String> prefix, Predicate<T> predicate, Module module)
    {
        this.config = requireNonNull(config, "config is null");
        this.prefix = requireNonNull(prefix, "prefix is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        T configuration;
        if (prefix.isPresent()) {
            configuration = buildConfigObject(config, prefix.get());
        }
        else {
            configuration = buildConfigObject(config);
        }
        if (predicate.test(configuration)) {
            install(module);
        }
    }
}
