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

import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.identity.DispatchSession;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class GalaxyTrinoSecurityControl
        implements TrinoSecurityControl
{
    private final TrinoSecurityApi securityApi;

    @Inject
    public GalaxyTrinoSecurityControl(TrinoSecurityApi securityApi)
    {
        this.securityApi = requireNonNull(securityApi, "securityApi is null");
    }

    @Override
    public void entityCreated(DispatchSession session, EntityId entityId)
    {
        securityApi.entityCreated(session, entityId);
    }

    @Override
    public void entityDropped(DispatchSession session, EntityId entityId)
    {
        securityApi.entityDropped(session, entityId);
    }
}
