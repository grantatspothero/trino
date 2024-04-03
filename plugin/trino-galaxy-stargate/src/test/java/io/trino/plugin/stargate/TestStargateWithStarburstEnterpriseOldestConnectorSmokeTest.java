/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.trino.plugin.stargate;

public class TestStargateWithStarburstEnterpriseOldestConnectorSmokeTest
        extends BaseStargateWithStarburstEnterpriseConnectorSmokeTest
{
    // Test with the oldest LTS https://docs.starburst.io/latest/versions.html
    public TestStargateWithStarburstEnterpriseOldestConnectorSmokeTest()
    {
        super("402-e.0");
    }
}
