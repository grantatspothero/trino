/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import io.trino.plugin.base.galaxy.GalaxySqlSocketFactory;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.Properties;

public class GalaxySqlServerSocketFactory
        extends GalaxySqlSocketFactory
{
    public GalaxySqlServerSocketFactory(String propertiesString)
    {
        super(parseProperties(propertiesString));
    }

    private static Properties parseProperties(String propertiesString)
    {
        try (StringReader reader = new StringReader(propertiesString)) {
            Properties properties = new Properties();
            properties.load(reader);
            return properties;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not parse argument properties", e);
        }
    }
}
