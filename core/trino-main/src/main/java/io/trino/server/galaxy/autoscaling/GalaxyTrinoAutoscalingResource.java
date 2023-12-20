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
package io.trino.server.galaxy.autoscaling;

import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

/**
 * Galaxy resources with debugMetrics for worker autoscaling - authenticated with the Operator token
 */
@Path("/v1/galaxy/autoscaling")
public class GalaxyTrinoAutoscalingResource
{
    private final WorkerRecommendationProvider recommendationProvider;

    @Inject
    public GalaxyTrinoAutoscalingResource(WorkerRecommendationProvider recommendationProvider)
    {
        this.recommendationProvider = requireNonNull(recommendationProvider, "recommendationProvider is null");
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(APPLICATION_JSON)
    public Response getWorkerRecommendation()
            throws WebApplicationException
    {
        return Response.ok(recommendationProvider.get()).build();
    }
}
