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
package io.trino.server.security.galaxy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.CreateEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.CreateRoleGrant;
import io.starburst.stargate.accesscontrol.client.EntityAlreadyExistsException;
import io.starburst.stargate.accesscontrol.client.EntityNotFoundException;
import io.starburst.stargate.accesscontrol.client.GalaxyPrincipal;
import io.starburst.stargate.accesscontrol.client.OperationNotAllowedException;
import io.starburst.stargate.accesscontrol.client.RevokeEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.RoleGrant;
import io.starburst.stargate.accesscontrol.client.TableGrant;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.identity.DispatchSession;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT;

/**
 * Specialized TrinoSecurityApi for use by the Galaxy indexer. Grants visibility to all
 * catalogs, schemas and tables solely so that they can be indexed. Actual permission
 * filtering is done when the index is queried by a user.
 */
public class GalaxyIndexerTrinoSecurityApi
        implements TrinoSecurityApi
{
    private static final ContentsVisibility ALLOW_ALL = new ContentsVisibility(GrantKind.ALLOW, ImmutableSet.of());

    @Override
    public ContentsVisibility getCatalogVisibility(DispatchSession session)
    {
        return ALLOW_ALL;
    }

    @Override
    public ContentsVisibility getSchemaVisibility(DispatchSession session, CatalogId catalogId)
    {
        return ALLOW_ALL;
    }

    @Override
    public Map<String, ContentsVisibility> getTableVisibility(DispatchSession session, CatalogId catalogId, Set<String> schemaNames)
    {
        return schemaNames.stream().collect(toImmutableMap(Function.identity(), ignore -> ALLOW_ALL));
    }

    @Override
    public EntityPrivileges getEntityPrivileges(DispatchSession session, RoleId roleId, EntityId entityId)
    {
        return new EntityPrivileges(new RoleName("indexer"), roleId, false, ImmutableSet.of(), ImmutableMap.of(SELECT.name(), ALLOW_ALL), ImmutableList.of());
    }

    @Override
    public Map<RoleName, RoleId> listEnabledRoles(DispatchSession session)
    {
        return ImmutableMap.of();
    }

    @Override
    public Map<RoleName, RoleId> listRoles(DispatchSession session)
            throws OperationNotAllowedException
    {
        return ImmutableMap.of();
    }

    // All operations below disallowed - only above methods are allowed which relate to visibility only

    @Override
    public boolean canExecuteFunction(DispatchSession session, EntityId functionId)
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    @Override
    public boolean roleExists(DispatchSession session, RoleName role)
            throws OperationNotAllowedException
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    @Override
    public void createRole(DispatchSession session, RoleName role)
            throws OperationNotAllowedException, EntityNotFoundException, EntityAlreadyExistsException
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    @Override
    public void dropRole(DispatchSession session, RoleName role)
            throws OperationNotAllowedException, EntityNotFoundException
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(DispatchSession session, GalaxyPrincipal principal, boolean transitive)
            throws OperationNotAllowedException
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    @Override
    public void grantRoles(DispatchSession session, Set<CreateRoleGrant> roleGrants)
            throws OperationNotAllowedException, EntityNotFoundException, EntityAlreadyExistsException
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    @Override
    public void revokeRoles(DispatchSession session, Set<RoleGrant> roleGrants)
            throws OperationNotAllowedException, EntityNotFoundException
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    @Override
    public void addEntityPrivileges(DispatchSession session, EntityId entityId, Set<CreateEntityPrivilege> privileges)
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    @Override
    public void revokeEntityPrivileges(DispatchSession session, EntityId entityId, Set<RevokeEntityPrivilege> privileges)
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    @Override
    public List<TableGrant> listTableGrants(DispatchSession session, EntityId entity)
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    @Override
    public void setEntityOwner(DispatchSession session, EntityId entityId, RoleName owner)
    {
        throw new OperationNotAllowedException("Operation disallowed for indexer");
    }

    // entity* methods are merely callbacks that are ignored

    @Override
    public void entityCreated(DispatchSession session, EntityId entityId)
    {
        // Ignore
    }

    @Override
    public <E extends EntityId> void entityRenamed(DispatchSession session, E entityId, E newEntityId)
    {
        // Ignore
    }

    @Override
    public void entityDropped(DispatchSession session, EntityId entityId)
    {
        // Ignore
    }
}
