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
package io.trino.server.metadataonly;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.server.security.ResourceSecurity;

import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.util.Optional;

import static io.trino.server.galaxy.GalaxyBearerToken.extractToken;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.util.Objects.requireNonNull;

@Path("/galaxy/metadata/v1/system")
public class MetadataOnlySystemResource
{
    private static final Logger log = Logger.get(MetadataOnlySystemResource.class);

    private final MetadataOnlySystemState systemState;
    private final Optional<String> shutdownAuthenticationKey;

    @Inject
    public MetadataOnlySystemResource(MetadataOnlySystemState systemState, MetadataOnlyConfig config)
    {
        this.systemState = requireNonNull(systemState, "systemState is null");
        shutdownAuthenticationKey = config.getShutdownAuthenticationKey();
    }

    // security is public because it is managed manually in the method via shutdownAuthenticationKey
    @ResourceSecurity(PUBLIC)
    @PUT
    @Path("shutdown")
    public void shutdown(@Context HttpHeaders httpHeaders)
    {
        boolean hasValidKey = shutdownAuthenticationKey.map(key -> extractToken(httpHeaders::getHeaderString, getClass().getName()).map(token -> token.equals(key)).orElse(false))
                .orElse(false);
        if (!hasValidKey) {
            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }

        log.info("Shutdown request received");

        systemState.setShuttingDown();
    }
}
