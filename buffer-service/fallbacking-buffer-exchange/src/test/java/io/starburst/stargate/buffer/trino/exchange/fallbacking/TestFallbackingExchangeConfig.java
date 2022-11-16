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
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;

class TestFallbackingExchangeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FallbackingExchangeConfig.class)
                .setFailureTrackingServiceUri(null)
                .setFallbackRecentFailuresCountThreshold(5)
                .setMinFallbackModeDuration(Duration.succinctDuration(5, MINUTES))
                .setMaxFallbackModeDuration(Duration.succinctDuration(15, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws URISyntaxException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("exchange.failure-tracking.uri", "http://some-failure-tracking-host:123")
                .put("exchange.fallback-recent-failures-count-threshold", "6")
                .put("exchange.min-fallback-duration", "6m")
                .put("exchange.max-fallback-duration", "16m")
                .buildOrThrow();

        FallbackingExchangeConfig expected = new FallbackingExchangeConfig()
                .setFailureTrackingServiceUri(new URI("http://some-failure-tracking-host:123"))
                .setFallbackRecentFailuresCountThreshold(6)
                .setMinFallbackModeDuration(Duration.succinctDuration(6, MINUTES))
                .setMaxFallbackModeDuration(Duration.succinctDuration(16, MINUTES));

        assertFullMapping(properties, expected);
    }
}
