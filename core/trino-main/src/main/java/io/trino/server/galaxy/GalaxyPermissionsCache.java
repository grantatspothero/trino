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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.QueryId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class GalaxyPermissionsCache
{
    private static final int CACHE_SIZE = 100;

    private final LoadingCache<QueryId, Map<DispatchSession, GalaxyQueryPermissions>> permissionsCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .build(CacheLoader.from(queryId -> new ConcurrentHashMap<>()));

    @Inject
    public GalaxyPermissionsCache() {}

    public <V> V withGalaxyPermissions(TrinoSecurityApi trinoSecurityApi, DispatchSession session, Optional<QueryId> optionalQueryId, Function<GalaxyQueryPermissions, V> permissionsFunction)
    {
        GalaxyQueryPermissions queryPermissions = optionalQueryId
                .map(queryId -> permissionsCache.getUnchecked(queryId)
                        .computeIfAbsent(session, ignore -> new GalaxyQueryPermissions(trinoSecurityApi, session)))
                .orElseGet(() -> new GalaxyQueryPermissions(trinoSecurityApi, session));
        return permissionsFunction.apply(queryPermissions);
    }

    public static class GalaxyQueryPermissions
    {
        private final TrinoSecurityApi trinoSecurityApi;
        private final DispatchSession session;
        private final Map<EntityPrivilegesKey, EntityPrivileges> entityPrivilegesMap = new HashMap<>();
        private Map<RoleName, RoleId> activeRoles;
        private Map<RoleId, RoleName> allRoles;
        private ContentsVisibility catalogVisibility;

        public GalaxyQueryPermissions(TrinoSecurityApi trinoSecurityApi, DispatchSession session)
        {
            this.trinoSecurityApi = requireNonNull(trinoSecurityApi, "trinoSecurityApi is null");
            this.session = requireNonNull(session, "session is null");
        }

        public synchronized Map<RoleName, RoleId> listEnabledRoles()
        {
            if (activeRoles == null) {
                activeRoles = ImmutableMap.copyOf(trinoSecurityApi.listEnabledRoles(session));
            }
            return activeRoles;
        }

        public synchronized String getRoleDisplayName(RoleId roleId)
        {
            if (allRoles == null) {
                allRoles = trinoSecurityApi.listRoles(session).entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));
            }
            RoleName roleName = allRoles.get(roleId);
            return roleName != null ? roleName.getName() : roleId.toString() + " (dropped)";
        }

        public synchronized EntityPrivileges getEntityPrivileges(RoleId roleId, EntityId entity)
        {
            return entityPrivilegesMap.computeIfAbsent(new EntityPrivilegesKey(roleId, entity), ignore -> trinoSecurityApi.getEntityPrivileges(session, roleId, entity));
        }

        public synchronized ContentsVisibility getCatalogVisibility()
        {
            if (catalogVisibility != null) {
                return catalogVisibility;
            }
            catalogVisibility = trinoSecurityApi.getCatalogVisibility(session);
            return catalogVisibility;
        }

        private record EntityPrivilegesKey(RoleId roleId, EntityId entity)
        {
            EntityPrivilegesKey
            {
                requireNonNull(roleId, "roleId is null");
                requireNonNull(entity, "entity is null");
            }
        }
    }
}
