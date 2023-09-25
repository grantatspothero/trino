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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.QueryId;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Streams.stream;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class GalaxyPermissionsCache
{
    // Currently, we allow at most 60 concurrent queries (20 queries and 40 "data definition"), this value is with some margin.
    private static final int EXPECTED_CONCURRENT_QUERIES = 100;
    private static final int QUERY_CACHE_SIZE = EXPECTED_CONCURRENT_QUERIES;

    private final LoadingCache<QueryId, Map<DispatchSession, GalaxyQueryPermissions>> permissionsCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(QUERY_CACHE_SIZE)
            .build(CacheLoader.from(queryId -> new ConcurrentHashMap<>()));

    public GalaxyQueryPermissions getCache(TrinoSecurityApi trinoSecurityApi, DispatchSession session, QueryId queryId)
    {
        return permissionsCache.getUnchecked(queryId)
                .computeIfAbsent(session, ignore -> new GalaxyQueryPermissions(trinoSecurityApi, session));
    }

    /**
     * Permission cache for given query and identity ({@link DispatchSession}).
     */
    @ThreadSafe
    public static class GalaxyQueryPermissions
    {
        private final TrinoSecurityApi trinoSecurityApi;
        private final DispatchSession session;

        // It's a cache to support concurrent loads while also preventing multiple loads for the same key
        private final LoadingCache<EntityPrivilegesKey, EntityPrivileges> entityPrivileges;
        @GuardedBy("this")
        private ContentsVisibility catalogVisibility;
        // It's a cache only for convenience to use loading cache's bulk loading capability
        private final LoadingCache<TableVisibilityKey, ContentsVisibility> tableVisibility;

        public GalaxyQueryPermissions(TrinoSecurityApi trinoSecurityApi, DispatchSession session)
        {
            this.trinoSecurityApi = requireNonNull(trinoSecurityApi, "trinoSecurityApi is null");
            this.session = requireNonNull(session, "session is null");

            entityPrivileges = buildNonEvictableCache(
                    CacheBuilder.newBuilder(),
                    CacheLoader.from(key -> trinoSecurityApi.getEntityPrivileges(session, key.roleId(), key.entity())));

            tableVisibility = buildNonEvictableCache(
                    CacheBuilder.newBuilder(),
                    BulkOnlyLoader.of(keys -> {
                        CatalogId catalogId = stream(keys)
                                .map(TableVisibilityKey::catalogId)
                                .distinct()
                                // The cache is never invoked for different catalogs at once
                                .collect(onlyElement());

                        Set<String> schemaNames = stream(keys)
                                .map(TableVisibilityKey::schemaName)
                                .collect(toImmutableSet());

                        Map<String, ContentsVisibility> visibility = trinoSecurityApi.getTableVisibility(session, catalogId, schemaNames);
                        return visibility.entrySet().stream()
                                .collect(toImmutableMap(entry -> new TableVisibilityKey(catalogId, entry.getKey()), Entry::getValue));
                    }));
        }

        public EntityPrivileges getEntityPrivileges(RoleId roleId, EntityId entity)
        {
            return entityPrivileges.getUnchecked(new EntityPrivilegesKey(roleId, entity));
        }

        public synchronized ContentsVisibility getCatalogVisibility()
        {
            if (catalogVisibility == null) {
                catalogVisibility = trinoSecurityApi.getCatalogVisibility(session);
            }
            return catalogVisibility;
        }

        public Map<String, ContentsVisibility> getTableVisibility(CatalogId catalogId, Set<String> schemaNames)
        {
            if (schemaNames.isEmpty()) {
                return ImmutableMap.of();
            }
            List<TableVisibilityKey> cacheKeys = schemaNames.stream()
                    .map(schemaName -> new TableVisibilityKey(catalogId, schemaName))
                    .collect(toImmutableList());
            Map<TableVisibilityKey, ContentsVisibility> loaded;
            try {
                loaded = tableVisibility.getAll(cacheKeys);
            }
            catch (ExecutionException e) { // Impossible, the cache loader does not currently throw checked exceptions
                throw new RuntimeException(e);
            }
            return loaded.entrySet().stream()
                    .collect(toImmutableMap(
                            entry -> {
                                checkArgument(entry.getKey().catalogId().equals(catalogId), "Unexpected CatalogId returned: %s, expected %s", entry.getKey().catalogId(), catalogId);
                                return entry.getKey().schemaName();
                            },
                            Entry::getValue));
        }

        private record EntityPrivilegesKey(RoleId roleId, EntityId entity)
        {
            EntityPrivilegesKey
            {
                requireNonNull(roleId, "roleId is null");
                requireNonNull(entity, "entity is null");
            }
        }

        private record TableVisibilityKey(CatalogId catalogId, String schemaName)
        {
            TableVisibilityKey
            {
                requireNonNull(catalogId, "catalogId is null");
                requireNonNull(schemaName, "schemaName is null");
            }
        }
    }
}
