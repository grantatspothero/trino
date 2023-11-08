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
package io.trino.tests.exchange.buffer;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Properties;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBufferExchangePluginVersion
{
    private static final int ALLOWED_VERSION_DRIFT = 1;

    /**
     * Ensure Buffer Service based exchange we are using is based on top of OSS version of SPI which
     * matches one which is currently used in stargate-trino.
     * We allow SPI to be one version newer assuming at least one release backward compatibility of SPI interfaces.
     * Allowing skew makes it easier to perform galaxy-trino updates after OSS release as we do not to need to bump
     * buffer service dependency as part of the update.
     */
    @Test
    public void testSPIVersionCompatibility()
            throws Exception
    {
        Properties properties = new Properties();
        try (InputStream inputStream = getResource(getClass(), "test-buffer-exchange-plugin-versions.properties").openStream()) {
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

        String bufferServiceVersion = properties.getProperty("dep.trino-buffer-service.version");
        int bufferServiceTrinoVersion = Integer.parseInt(bufferServiceVersion.split("-")[0]);

        assertThat(baseTrinoVersion - bufferServiceTrinoVersion)
                .withFailMessage(
                        "Buffer Service version [%s] does not match the base Trino version [%s] the Galaxy Trino [%s] is based on (allowed SPI version drift is %s)",
                        bufferServiceVersion,
                        baseTrinoVersion,
                        galaxyTrinoVersion,
                        ALLOWED_VERSION_DRIFT)
                .isLessThanOrEqualTo(ALLOWED_VERSION_DRIFT)
                .isGreaterThanOrEqualTo(0);
    }

    @Test
    public void testJacksonVersionCompatibility()
            throws Exception
    {
        // Ensure runtime version of Jackson library in Trino matches compile time version of Jackson for buffer service based exchange
        Properties bufferServiceDependenciesVersions = new Properties();
        try (InputStream inputStream = getResource(getClass(), "/io/starburst/stargate/buffer/trino/exchange/dependencies-versions.properties").openStream()) {
            bufferServiceDependenciesVersions.load(inputStream);
        }

        Properties trinoDependenciesVerions = new Properties();
        try (InputStream inputStream = getResource(getClass(), "test-buffer-exchange-plugin-versions.properties").openStream()) {
            trinoDependenciesVerions.load(inputStream);
        }

        String bufferServiceJacksonVersion = bufferServiceDependenciesVersions.getProperty("dep.jackson.version");
        String trinoJacksonVersion = trinoDependenciesVerions.getProperty("dep.jackson.version");

        assertThat(trinoJacksonVersion).as("buffer service jackson version").isEqualTo(bufferServiceJacksonVersion);
    }
}
