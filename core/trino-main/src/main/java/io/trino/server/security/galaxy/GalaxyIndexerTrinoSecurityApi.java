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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.cache.CacheKeyAndResult;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.CreateEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.CreateRoleGrant;
import io.starburst.stargate.accesscontrol.client.EntityAlreadyExistsException;
import io.starburst.stargate.accesscontrol.client.EntityNotFoundException;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionDetails;
import io.starburst.stargate.accesscontrol.client.GalaxyPrincipal;
import io.starburst.stargate.accesscontrol.client.OperationNotAllowedException;
import io.starburst.stargate.accesscontrol.client.RevokeEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.RoleGrant;
import io.starburst.stargate.accesscontrol.client.TableGrant;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.ColumnId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.EntityKind;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import io.starburst.stargate.identity.DispatchSession;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.stargate.accesscontrol.client.ContentsVisibility.ALLOW_ALL;
import static io.starburst.stargate.accesscontrol.client.ContentsVisibility.DENY_ALL;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT;
import static java.util.Objects.requireNonNull;

/**
 * Specialized TrinoSecurityApi for use by the Galaxy indexer. Grants visibility to all
 * catalogs, schemas and tables solely so that they can be indexed. Actual permission
 * filtering is done when the index is queried by a user.
 */
public class GalaxyIndexerTrinoSecurityApi
        implements TrinoSecurityApi
{
    private final BiMap<CatalogId, String> allCatalogs;
    private final Map<CatalogId, SharedSchemaNameAndAccepted> sharedCatalogs;

    public GalaxyIndexerTrinoSecurityApi(BiMap<CatalogId, String> allCatalogs, Map<CatalogId, SharedSchemaNameAndAccepted> sharedCatalogs)
    {
        this.allCatalogs = ImmutableBiMap.copyOf(requireNonNull(allCatalogs, "allCatalogs is null"));
        this.sharedCatalogs = ImmutableMap.copyOf(requireNonNull(sharedCatalogs, "sharedCatalogs is null"));
    }

    @Override
    public ContentsVisibility getCatalogVisibility(DispatchSession session)
    {
        if (sharedCatalogs.isEmpty()) {
            return ALLOW_ALL;
        }
        return new ContentsVisibility(GrantKind.DENY, getCatalogVisibility(allCatalogs.keySet()));
    }

    @Override
    public ContentsVisibility getCatalogVisibility(DispatchSession session, Set<CatalogId> catalogIds)
    {
        if (sharedCatalogs.isEmpty()) {
            return ALLOW_ALL;
        }
        return new ContentsVisibility(GrantKind.DENY, getCatalogVisibility(catalogIds));
    }

    @Override
    public ContentsVisibility getSchemaVisibility(DispatchSession session, CatalogId catalogId)
    {
        if (sharedCatalogs.isEmpty()) {
            return ALLOW_ALL;
        }
        SharedSchemaNameAndAccepted sharedSchema = sharedCatalogs.get(catalogId);
        if (sharedSchema == null) {
            return ALLOW_ALL;
        }
        return new ContentsVisibility(GrantKind.DENY, getSchemaVisibility(catalogId, ImmutableSet.of(sharedSchema.schemaName(), "information_schema")));
    }

    @Override
    public ContentsVisibility getVisibilityForSchemas(DispatchSession session, CatalogId catalogId, Set<String> schemaNames)
    {
        if (!sharedCatalogs.containsKey(catalogId)) {
            return ALLOW_ALL;
        }
        return new ContentsVisibility(GrantKind.DENY, getSchemaVisibility(catalogId, schemaNames));
    }

    @Override
    public Map<String, ContentsVisibility> getTableVisibility(DispatchSession session, CatalogId catalogId, Set<String> schemaNames)
    {
        SharedSchemaNameAndAccepted state = sharedCatalogs.get(catalogId);
        if (state == null) {
            return schemaNames.stream().collect(toImmutableMap(Function.identity(), ignore -> ALLOW_ALL));
        }
        return schemaNames.stream().collect(toImmutableMap(Function.identity(), schema -> state.accepted() && state.schemaName().equals(schema) ? ALLOW_ALL : DENY_ALL));
    }

    @Override
    public ContentsVisibility getVisibilityForTables(DispatchSession session, CatalogId catalogId, String schemaName, Set<String> tableNames)
    {
        SharedSchemaNameAndAccepted state = sharedCatalogs.get(catalogId);
        if (state == null || (state.accepted() && schemaName.equals(state.schemaName()))) {
            return ALLOW_ALL;
        }
        return DENY_ALL;
    }

    @Override
    public EntityPrivileges getEntityPrivileges(DispatchSession session, RoleId roleId, EntityId entityId)
    {
        EntityKind kind = entityId.getEntityKind();
        if (kind.isContainerKind() || kind.isContainedKind()) {
            CatalogId catalogId = entityId.getCatalogId();
            SharedSchemaNameAndAccepted state = sharedCatalogs.get(catalogId);
            if (state != null && kind.isContainedKind() && (!state.accepted() || !state.schemaName().equals(entityId.getSchemaName()))) {
                // Return no privileges
                return new EntityPrivileges(new RoleName("indexer"), roleId, true, false, ImmutableSet.of(), ImmutableMap.of(), ImmutableList.of(), ImmutableMap.of());
            }
        }
        // return EntityPrivileges with explicitOwner set to true. This is needed so that any checks for querying/inspecting will pass
        // Indexer does not do any RBAC checks and, instead, those are done at query time by Portal
        return new EntityPrivileges(new RoleName("indexer"), roleId, true, false, ImmutableSet.of(), ImmutableMap.of(SELECT.name(), ALLOW_ALL), ImmutableList.of(), ImmutableMap.of());
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

    @Override
    public void columnTypeChanged(DispatchSession session, ColumnId columnId, String oldType, String newType)
    {
        // Ignore
    }

    @Override
    public boolean canUseLocation(DispatchSession session, String location)
    {
        return true;
    }

    @Override
    public Set<GalaxyLanguageFunctionDetails> getAvailableFunctions(DispatchSession session)
    {
        return ImmutableSet.of();
    }

    @Override
    public List<CacheKeyAndResult> validateCachedResults(DispatchSession session, List<CacheKeyAndResult> cacheReferences)
    {
        return ImmutableList.of();
    }

    private Set<String> getCatalogVisibility(Set<CatalogId> catalogIds)
    {
        return catalogIds.stream()
                .filter(this::isVisibleCatalogId)
                .map(allCatalogs::get)
                .filter(Objects::nonNull)
                .collect(toImmutableSet());
    }

    private boolean isVisibleCatalogId(CatalogId catalogId)
    {
        SharedSchemaNameAndAccepted state = sharedCatalogs.get(catalogId);
        return state == null || state.accepted();
    }

    private Set<String> getSchemaVisibility(CatalogId catalogId, Set<String> schemaNames)
    {
        SharedSchemaNameAndAccepted state = sharedCatalogs.get(catalogId);
        if (state == null) {
            return schemaNames;
        }
        if (!state.accepted() || !schemaNames.contains(state.schemaName())) {
            return ImmutableSet.of();
        }
        if (schemaNames.contains("information_schema")) {
            return ImmutableSet.of(state.schemaName(), "information_schema");
        }
        return ImmutableSet.of(state.schemaName());
    }
}
