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

import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

public class TestingStarburstEnterpriseServer
        extends TrinoContainer
{
    public TestingStarburstEnterpriseServer(String version)
    {
        super(DockerImageName.parse("starburstdata/starburst-enterprise:" + version).asCompatibleSubstituteFor("trinodb/trino"));
        addExposedPort(8080);
        start();
    }
}
