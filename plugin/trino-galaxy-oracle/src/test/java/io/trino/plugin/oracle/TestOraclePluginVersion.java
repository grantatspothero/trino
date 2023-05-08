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
package io.trino.plugin.oracle;

import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOraclePluginVersion
{
    @Test
    public void testVersionCompatibility()
            throws Exception
    {
        Properties properties = new Properties();
        try (InputStream inputStream = getResource(getClass(), "test-oracle-plugin-versions.properties").openStream()) {
            properties.load(inputStream);
        }

        String galaxyTrinoVersion = properties.getProperty("project.version");
        String pluginsVersion = properties.getProperty("dep.starburst-trino-plugins.version");

        String baseTrinoVersion = galaxyTrinoVersion.replaceFirst("-galaxy-1-SNAPSHOT$", "");
        checkState(!baseTrinoVersion.equals(galaxyTrinoVersion), "Galaxy Trino version does not match the expected pattern: [%s]", galaxyTrinoVersion);

        String basePluginsVersion = pluginsVersion.replaceFirst("\\..*", "");

        assertThat(basePluginsVersion)
                .withFailMessage(
                        "Plugins version [%s] does not match the base Trino version [%s] the Galaxy Trino [%s] is based on." +
                                " See this test's documentation for more information. " +
                                "TL;DR is: trino-hive and trino-base-jdbc have no backward/forward compatibility guarantees with respect to plugins extending it.",
                        pluginsVersion,
                        baseTrinoVersion,
                        galaxyTrinoVersion)
                .isEqualTo(baseTrinoVersion);
    }
}
