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
package io.trino.server;

import com.google.inject.Inject;
import io.trino.metadata.NodeState;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Galaxy version of the {@link ServerInfoResource} authenticated with the Operator token
 */
@Path("/v1/galaxy/info")
public class GalaxyServerInfoResource
{
    private final GracefulShutdownHandler shutdownHandler;

    @Inject
    public GalaxyServerInfoResource(GracefulShutdownHandler shutdownHandler)
    {
        this.shutdownHandler = requireNonNull(shutdownHandler, "shutdownHandler is null");
    }

    @ResourceSecurity(PUBLIC)
    @PUT
    @Path("state")
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    public Response updateState(NodeState state)
            throws WebApplicationException
    {
        if (state == NodeState.SHUTTING_DOWN) {
            shutdownHandler.requestShutdown();
            return Response.ok().build();
        }

        throw new WebApplicationException(
                Response.status(BAD_REQUEST)
                        .type(TEXT_PLAIN)
                        .entity(format("Invalid state transition to %s", state))
                        .build());
    }
}
