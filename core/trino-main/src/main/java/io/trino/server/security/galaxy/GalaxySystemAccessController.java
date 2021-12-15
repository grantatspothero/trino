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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.FunctionId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.spi.QueryId;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.SystemSecurityContext;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.server.security.galaxy.GalaxyIdentity.getContextRoleId;
import static io.trino.server.security.galaxy.GalaxyIdentity.toDispatchSession;
import static java.util.Objects.requireNonNull;

public class GalaxySystemAccessController
{
    private static final int CACHE_SIZE = 100;

    private final TrinoSecurityApi accessControlClient;
    private final CatalogIds catalogIds;
    private final LoadingCache<QueryId, Map<DispatchSession, GalaxyQueryPermissions>> permissionsCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .build(CacheLoader.from(queryId -> new ConcurrentHashMap<>()));

    @Inject
    public GalaxySystemAccessController(TrinoSecurityApi accessControlClient, CatalogIds catalogIds)
    {
        this.accessControlClient = requireNonNull(accessControlClient, "accessControlClient is null");
        this.catalogIds = requireNonNull(catalogIds, "catalogIds is null");
    }

    public Optional<CatalogId> getCatalogId(String catalogName)
    {
        return catalogIds.getCatalogId(catalogName);
    }

    public boolean isReadOnlyCatalog(String catalogName)
    {
        return catalogIds.isReadOnlyCatalog(catalogName);
    }

    public EntityPrivileges getEntityPrivileges(SystemSecurityContext context, EntityId entity)
    {
        return withGalaxyPermissions(context, permissions -> permissions.getEntityPrivileges(getContextRoleId(context), entity));
    }

    public Map<RoleName, RoleId> listEnabledRoles(SystemSecurityContext context)
    {
        return withGalaxyPermissions(context, GalaxyQueryPermissions::listEnabledRoles);
    }

    public Predicate<String> getCatalogVisibility(SystemSecurityContext context)
    {
        ContentsVisibility catalogVisibility = accessControlClient.getCatalogVisibility(toDispatchSession(context.getIdentity()));
        return catalogName -> catalogIds.getCatalogId(catalogName)
                .map(CatalogId::toString)
                .map(catalogVisibility::isVisible)
                .orElse(false);
    }

    public Predicate<String> getSchemaVisibility(SystemSecurityContext context, CatalogId catalogId)
    {
        // This is only called once per query, so no need to cache
        return accessControlClient.getSchemaVisibility(toDispatchSession(context.getIdentity()), catalogId)::isVisible;
    }

    public Predicate<SchemaTableName> getTableVisibility(SystemSecurityContext context, CatalogId catalogId, Set<String> schemaNames)
    {
        // This is only called once per query, so no need to cache
        if (schemaNames.isEmpty()) {
            return name -> false;
        }

        Map<String, ContentsVisibility> tableVisibility = accessControlClient.getTableVisibility(toDispatchSession(context.getIdentity()), catalogId, schemaNames);
        return name -> {
            ContentsVisibility contentsVisibility = tableVisibility.get(name.getSchemaName());
            return contentsVisibility != null && contentsVisibility.isVisible(name.getTableName());
        };
    }

    public String getRoleDisplayName(SystemSecurityContext context, RoleId roleId)
    {
        return withGalaxyPermissions(context, permissions -> permissions.getRoleDisplayName(roleId));
    }

    public boolean canExecuteFunction(SystemSecurityContext context, FunctionId functionId)
    {
        return accessControlClient.canExecuteFunction(toDispatchSession(context.getIdentity()), functionId);
    }

    private <V> V withGalaxyPermissions(SystemSecurityContext context, Function<GalaxyQueryPermissions, V> permissionsFunction)
    {
        DispatchSession session = toDispatchSession(context.getIdentity());
        GalaxyQueryPermissions queryPermissions = context.getQueryId()
                .map(queryId -> permissionsCache.getUnchecked(queryId)
                        .computeIfAbsent(session, GalaxyQueryPermissions::new))
                .orElseGet(() -> new GalaxyQueryPermissions(session));
        return permissionsFunction.apply(queryPermissions);
    }

    private class GalaxyQueryPermissions
    {
        private final DispatchSession session;
        private final Map<EntityId, EntityPrivileges> entityPrivilegesMap = new HashMap<>();
        private Map<RoleName, RoleId> activeRoles;
        private Map<RoleId, RoleName> allRoles;

        public GalaxyQueryPermissions(DispatchSession session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        public DispatchSession getSession()
        {
            return session;
        }

        public synchronized Map<RoleName, RoleId> listEnabledRoles()
        {
            if (activeRoles == null) {
                activeRoles = ImmutableMap.copyOf(accessControlClient.listEnabledRoles(session));
            }
            return activeRoles;
        }

        public synchronized String getRoleDisplayName(RoleId roleId)
        {
            if (allRoles == null) {
                allRoles = accessControlClient.listRoles(session).entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));
            }
            RoleName roleName = allRoles.get(roleId);
            return roleName != null ? roleName.getName() : roleId.toString() + " (dropped)";
        }

        public synchronized EntityPrivileges getEntityPrivileges(RoleId roleId, EntityId entity)
        {
            return entityPrivilegesMap.computeIfAbsent(entity, ignore -> accessControlClient.getEntityPrivileges(session, roleId, entity));
        }
    }
}
