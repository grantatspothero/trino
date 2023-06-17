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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.discovery.client.failures.FailureInfo;
import io.starburst.stargate.buffer.discovery.client.failures.FailureTrackingApi;
import io.starburst.stargate.buffer.discovery.client.failures.FailuresStatusInfo;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static com.google.common.primitives.Longs.min;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.trino.exchange.fallbacking.ExchangeMode.BUFFER;
import static io.starburst.stargate.buffer.trino.exchange.fallbacking.ExchangeMode.FILESYSTEM;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FallbackSelector
{
    private static final Logger log = Logger.get(FallbackSelector.class);

    private final ExceptionAnalyzer exceptionAnalyzer;
    private final FailureTrackingApi failureTrackingApi;
    private final int failuresCountThreshold;
    private final Duration minFallbackModeDuration;
    private final Duration maxFallbackModeDuration;
    private final Ticker ticker;

    @GuardedBy("this")
    private Optional<Duration> currentFallbackDuration = Optional.empty();
    @GuardedBy("this")
    private Optional<Stopwatch> currentFallbackStopwatch = Optional.empty();

    @Inject
    public FallbackSelector(ExceptionAnalyzer exceptionAnalyzer, FailureTrackingApi failureTrackingApi, FallbackingExchangeConfig config)
    {
        this(exceptionAnalyzer, failureTrackingApi, config, Ticker.systemTicker());
    }

    @VisibleForTesting
    FallbackSelector(ExceptionAnalyzer exceptionAnalyzer, FailureTrackingApi failureTrackingApi, FallbackingExchangeConfig config, Ticker ticker)
    {
        this.exceptionAnalyzer = requireNonNull(exceptionAnalyzer, "exceptionAnalyzer is null");
        this.failureTrackingApi = requireNonNull(failureTrackingApi, "failureTrackingApi is null");
        this.failuresCountThreshold = config.getFallbackRecentFailuresCountThreshold();
        this.minFallbackModeDuration = config.getMinFallbackModeDuration();
        this.maxFallbackModeDuration = config.getMaxFallbackModeDuration();
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    public void registerBufferExchangeFailure(Throwable throwable)
    {
        FailureInfo failureInfo = new FailureInfo(
                Optional.empty(), // todo
                "node_info", // todo (can we get this?)
                Throwables.getStackTraceAsString(throwable));
        try {
            if (exceptionAnalyzer.isInternalException(throwable)) {
                failureTrackingApi.registerFailure(failureInfo);
            }
        }
        catch (RuntimeException e) {
            log.warn(e, "could not report exchange failure; failureInfo=%s", failureInfo);
        }
    }

    public <T> T handleCall(Supplier<T> callable)
    {
        try {
            return callable.get();
        }
        catch (RuntimeException e) {
            registerBufferExchangeFailure(e);
            throw e;
        }
    }

    public void handleCall(Runnable runnable)
    {
        try {
            runnable.run();
        }
        catch (RuntimeException e) {
            registerBufferExchangeFailure(e);
            throw e;
        }
    }

    public <T> CompletableFuture<T> handleFutureCall(Supplier<CompletableFuture<T>> function)
    {
        return handleFuture(handleCall(function));
    }

    private <T> CompletableFuture<T> handleFuture(CompletableFuture<T> future)
    {
        return future.handle(
                (value, t) -> {
                    if (t != null) {
                        registerBufferExchangeFailure(t);
                        Throwables.throwIfUnchecked(t);
                        throw new IllegalArgumentException("Expected unchecked exception", t);
                    }
                    return value;
                });
    }

    public synchronized ExchangeMode getExchangeMode()
    {
        if (currentFallbackDuration.isPresent()) {
            verify(currentFallbackStopwatch.isPresent());
            if (currentFallbackStopwatch.get().elapsed().toMillis() < currentFallbackDuration.get().toMillis()) {
                return FILESYSTEM;
            }
        }

        FailuresStatusInfo failuresStatus;
        try {
            failuresStatus = failureTrackingApi.getFailuresStatus();
        }
        catch (Exception e) {
            log.warn(e, "Could not get failures status");
            enterOrProlongFallback();
            return FILESYSTEM;
        }

        if (failuresStatus.recentFailuresCount() > failuresCountThreshold) {
            log.info("Entering or prolonging FILESYSTEM exchange fallback mode; recent failuresCount=" + failuresStatus.recentFailuresCount());
            enterOrProlongFallback();
            return FILESYSTEM;
        }

        if (currentFallbackDuration.isPresent()) {
            log.info("Leaving FILESYSTEM exchange fallback mode; recent failuresCount=" + failuresStatus.recentFailuresCount());
        }
        resetFallback();
        return BUFFER;
    }

    @GuardedBy("this")
    private void enterOrProlongFallback()
    {
        if (currentFallbackDuration.isEmpty()) {
            currentFallbackDuration = Optional.of(minFallbackModeDuration);
        }
        else {
            currentFallbackDuration = Optional.of(succinctDuration(min(currentFallbackDuration.get().toMillis() * 2, maxFallbackModeDuration.toMillis()), MILLISECONDS));
        }
        currentFallbackStopwatch = Optional.of(Stopwatch.createStarted(ticker));
    }

    @GuardedBy("this")
    private void resetFallback()
    {
        currentFallbackDuration = Optional.empty();
        currentFallbackStopwatch = Optional.empty();
    }
}
