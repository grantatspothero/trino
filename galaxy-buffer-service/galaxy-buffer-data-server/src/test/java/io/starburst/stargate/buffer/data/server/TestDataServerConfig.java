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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDataServerConfig
{
    @Test
    public void assertDefaults()
    {
        assertRecordedDefaults(recordDefaults(DataServerConfig.class)
                .setDataIntegrityVerificationEnabled(true)
                .setTestingDropUploadedPages(false)
                .setHttpResponseThreads(100)
                .setTestingEnableStatsLogging(false)
                .setBroadcastInterval(succinctDuration(5, SECONDS))
                .setMinDrainingDuration(succinctDuration(30, SECONDS))
                .setDrainingMaxAttempts(4)
                .setMaxInProgressAddDataPagesRequests(150)
                .setInProgressAddDataPagesRequestsRateLimitThreshold(110)
                .setInProgressAddDataPagesRequestsThrottlingCounterDecayDuration(succinctDuration(5, SECONDS))
                .setChunkListTargetSize(1)
                .setChunkListMaxSize(100)
                .setChunkListPollTimeout(succinctDuration(100, MILLISECONDS))
                .setTraceResourceReportInterval(succinctDuration(5, MINUTES))
                .setTraceResourceMaximumReportsPerExchange(20)
                .setTrinoPlaneId(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("data-integrity-verification-enabled", "false")
                .put("testing.drop-uploaded-pages", "true")
                .put("http-response-threads", "88")
                .put("testing.enable-stats-logging", "true")
                .put("discovery-broadcast-interval", "102ms")
                .put("draining.min-duration", "103s")
                .put("draining.max-attempts", "5")
                .put("max-in-progress-add-data-pages-requests", "600")
                .put("in-progress-add-data-pages-requests-rate-limit-threshold", "400")
                .put("in-progress-add-data-pages-requests-throttling-counter-decay-duration", "25s")
                .put("chunk-list.target-size", "42")
                .put("chunk-list.max-size", "1000")
                .put("chunk-list.poll-timeout", "12345ms")
                .put("trace-resource-report-interval", "2m")
                .put("trace-resource-maximum-reports-per-exchange", "5")
                .put("trino.plane-id", "aws-us-east1-1")
                .buildOrThrow();

        DataServerConfig expected = new DataServerConfig()
                .setDataIntegrityVerificationEnabled(false)
                .setTestingDropUploadedPages(true)
                .setHttpResponseThreads(88)
                .setTestingEnableStatsLogging(true)
                .setBroadcastInterval(succinctDuration(102, MILLISECONDS))
                .setMinDrainingDuration(succinctDuration(103, SECONDS))
                .setDrainingMaxAttempts(5)
                .setMaxInProgressAddDataPagesRequests(600)
                .setInProgressAddDataPagesRequestsRateLimitThreshold(400)
                .setInProgressAddDataPagesRequestsThrottlingCounterDecayDuration(succinctDuration(25, SECONDS))
                .setChunkListTargetSize(42)
                .setChunkListMaxSize(1000)
                .setChunkListPollTimeout(succinctDuration(12345, MILLISECONDS))
                .setTraceResourceReportInterval(succinctDuration(2, MINUTES))
                .setTraceResourceMaximumReportsPerExchange(5)
                .setTrinoPlaneId("aws-us-east1-1");

        assertFullMapping(properties, expected);
    }
}