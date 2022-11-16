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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.Duration.succinctDuration;

public class FallbackingExchangeConfig
{
    private URI failureTrackingServiceUri;
    private int fallbackRecentFailuresCountThreshold = 5;
    private Duration minFallbackModeDuration = succinctDuration(5, TimeUnit.MINUTES);
    private Duration maxFallbackModeDuration = succinctDuration(15, TimeUnit.MINUTES);

    @NotNull
    public URI getFailureTrackingServiceUri()
    {
        return failureTrackingServiceUri;
    }

    @Config("exchange.failure-tracking.uri")
    @ConfigDescription("Failure tracking service URI")
    public FallbackingExchangeConfig setFailureTrackingServiceUri(URI failureTrackingServiceUri)
    {
        this.failureTrackingServiceUri = failureTrackingServiceUri;
        return this;
    }

    @Config("exchange.fallback-recent-failures-count-threshold")
    @ConfigDescription("Threshold on recent failures count reported by failure tracking service, when we enter mode to fallback to filesystem exchange")
    public FallbackingExchangeConfig setFallbackRecentFailuresCountThreshold(int fallbackRecentFailuresCountThreshold)
    {
        this.fallbackRecentFailuresCountThreshold = fallbackRecentFailuresCountThreshold;
        return this;
    }

    @Min(1)
    public int getFallbackRecentFailuresCountThreshold()
    {
        return fallbackRecentFailuresCountThreshold;
    }

    public Duration getMinFallbackModeDuration()
    {
        return minFallbackModeDuration;
    }

    @Config("exchange.min-fallback-duration")
    public FallbackingExchangeConfig setMinFallbackModeDuration(Duration minFallbackModeDuration)
    {
        this.minFallbackModeDuration = minFallbackModeDuration;
        return this;
    }

    public Duration getMaxFallbackModeDuration()
    {
        return maxFallbackModeDuration;
    }

    @Config("exchange.max-fallback-duration")
    public FallbackingExchangeConfig setMaxFallbackModeDuration(Duration maxFallbackModeDuration)
    {
        this.maxFallbackModeDuration = maxFallbackModeDuration;
        return this;
    }
}
