/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestStargateConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StargateConfig.class)
                .setAuthenticationType("PASSWORD")
                .setSslEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("stargate.authentication.type", "NONE")
                .put("ssl.enabled", "true")
                .buildOrThrow();

        StargateConfig expected = new StargateConfig()
                .setAuthenticationType("NONE")
                .setSslEnabled(true);

        assertFullMapping(properties, expected);
    }
}