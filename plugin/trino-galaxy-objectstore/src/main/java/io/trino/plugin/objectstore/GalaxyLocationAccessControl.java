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
package io.trino.plugin.objectstore;

import com.google.inject.Inject;
import io.starburst.stargate.accesscontrol.client.TrinoLocationApi;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map.Entry;

import static io.trino.plugin.base.galaxy.GalaxyIdentity.toDispatchSession;
import static java.util.Objects.requireNonNull;

public class GalaxyLocationAccessControl
        implements LocationAccessControl
{
    private final TrinoLocationApi trinoLocationApi;
    private final TrinoSecurityApi trinoSecurityApi;

    @Inject
    public GalaxyLocationAccessControl(TrinoLocationApi trinoLocationApi, TrinoSecurityApi trinoSecurityApi)
    {
        this.trinoLocationApi = requireNonNull(trinoLocationApi, "trinoLocationApi is null");
        this.trinoSecurityApi = requireNonNull(trinoSecurityApi, "trinoSecurityApi is null");
    }

    @Override
    public void checkCanUseLocation(ConnectorIdentity identity, String location)
    {
        DispatchSession session = toDispatchSession(identity);
        if (!trinoLocationApi.canUseLocation(session, location)) {
            RoleId roleId = session.getRoleId();
            String roleName = trinoSecurityApi.listRoles(session).entrySet().stream()
                    .filter(entry -> roleId.equals(entry.getValue()))
                    .map(Entry::getKey)
                    .findFirst()
                    .map(RoleName::toString)
                    .orElse(roleId.toString());
            throw new AccessDeniedException("Role %s is not allowed to use location: %s".formatted(roleName, location));
        }
    }
}
