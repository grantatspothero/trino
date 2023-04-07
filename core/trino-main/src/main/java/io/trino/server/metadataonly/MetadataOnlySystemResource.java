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

import io.airlift.log.Logger;
import io.trino.server.security.ResourceSecurity;

import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.util.Objects.requireNonNull;

@Path("/galaxy/metadata/v1/system")
public class MetadataOnlySystemResource
{
    private static final Logger log = Logger.get(MetadataOnlySystemResource.class);

    private final MetadataOnlySystemState systemState;

    @Inject
    public MetadataOnlySystemResource(MetadataOnlySystemState systemState)
    {
        this.systemState = requireNonNull(systemState, "systemState is null");
    }

    @PUT
    @Path("shutdown")
    @ResourceSecurity(PUBLIC)
    public void shutdown()
    {
        log.info("Shutdown request received");

        systemState.setShuttingDown();
    }
}
