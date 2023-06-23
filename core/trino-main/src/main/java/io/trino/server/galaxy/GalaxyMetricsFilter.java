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

import com.google.inject.Inject;
import io.airlift.log.Logger;

import javax.annotation.Priority;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;

import java.io.IOException;
import java.util.Optional;

import static io.trino.server.galaxy.GalaxyBearerToken.extractToken;
import static java.util.Objects.requireNonNull;

@Priority(Priorities.AUTHENTICATION)
public class GalaxyMetricsFilter
        implements ContainerRequestFilter
{
    private static final Logger log = Logger.get(GalaxyMetricsFilter.class);
    private static final String METRICS_ENDPOINT = "metrics";
    private final String metricsToken;

    @Inject
    public GalaxyMetricsFilter(GalaxyMetricsConfig galaxyMetricsConfig)
    {
        this.metricsToken = requireNonNull(galaxyMetricsConfig.getMetricsAuthenticationToken(), "galaxyMetricsConfig cannot be null");
    }

    @Override
    public void filter(ContainerRequestContext containerRequestContext)
            throws IOException
    {
        if (METRICS_ENDPOINT.equals(containerRequestContext.getUriInfo().getPath())) {
            if (metricsToken.isEmpty()) {
                log.error("metrics access is disabled");
                throw new ForbiddenException("Access disabled");
            }
            MultivaluedMap<String, String> headers = containerRequestContext.getHeaders();
            Optional<String> token = extractToken(headers::getFirst, getClass().getName());
            if (token.isEmpty() || !token.equals(Optional.of(metricsToken))) {
                log.error("Invalid token");
                throw new ForbiddenException("Invalid authentication");
            }
        }
    }
}
