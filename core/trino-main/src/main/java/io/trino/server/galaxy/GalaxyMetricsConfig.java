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
package io.trino.server.galaxy;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

public class GalaxyMetricsConfig
{
    private String metricsAuthenticationToken = "";

    @NotNull
    // An empty token disables access to /metrics
    @Pattern(regexp = "^$|[0-9a-zA-Z]{16,}")
    public String getMetricsAuthenticationToken()
    {
        return metricsAuthenticationToken;
    }

    @Config("galaxy.metrics.token")
    @ConfigDescription("Secret used for scraping metrics")
    @ConfigSecuritySensitive
    public GalaxyMetricsConfig setMetricsAuthenticationToken(String metricsAuthenticationToken)
    {
        this.metricsAuthenticationToken = metricsAuthenticationToken;
        return this;
    }
}
