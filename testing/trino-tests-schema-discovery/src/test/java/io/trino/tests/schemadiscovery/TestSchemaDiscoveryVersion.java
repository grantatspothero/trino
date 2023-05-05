/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.schemadiscovery;

import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSchemaDiscoveryVersion
{
    private static final int ALLOWED_VERSION_DRIFT = 1;

    /**
     * Ensure Schema Discovery we are using is based on top of OSS version of SPI which
     * matches one which is currently used in stargate-trino.
     * We allow SPI to be one version newer assuming at least one release backward compatibility of SPI interfaces.
     * Allowing skew makes it easier to perform galaxy-trino updates after OSS release as we do not to need to bump
     * schema discovery dependency as part of the update.
     */
    @Test
    public void testVersionCompatibility()
            throws Exception
    {
        Properties properties = new Properties();
        try (InputStream inputStream = getResource(getClass(), "test-schema-discovery-versions.properties").openStream()) {
            properties.load(inputStream);
        }

        String galaxyTrinoVersion = properties.getProperty("project.version");
        int baseTrinoVersion;
        try {
            baseTrinoVersion = Integer.parseInt(galaxyTrinoVersion.replaceFirst("-galaxy-1-SNAPSHOT$", ""));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Galaxy Trino version does not match the expected pattern: [%s]".formatted(galaxyTrinoVersion), e);
        }

        String schemaDiscoveryVersion = properties.getProperty("dep.schema-discovery.version");
        int schemaDiscoveryTrinoVersion = Integer.parseInt(schemaDiscoveryVersion.split("\\.")[0]);

        assertThat(baseTrinoVersion - schemaDiscoveryTrinoVersion)
                .withFailMessage(
                        "Schema Discovery version [%s] does not match the base Trino version [%s] the Galaxy Trino [%s] is based on (allowed SPI version drift is %s)",
                        schemaDiscoveryVersion,
                        baseTrinoVersion,
                        galaxyTrinoVersion,
                        ALLOWED_VERSION_DRIFT)
                .isLessThanOrEqualTo(ALLOWED_VERSION_DRIFT)
                .isGreaterThanOrEqualTo(0);
    }
}
