/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.starburstdata.trino.plugin.license.LicenseManager;
import io.trino.plugin.jdbc.JdbcPlugin;

public class TestingStarburstOraclePlugin
        extends JdbcPlugin
{
    public TestingStarburstOraclePlugin(LicenseManager licenseManager)
    {
        super("oracle", new StarburstOracleClientModule(licenseManager));
    }
}
