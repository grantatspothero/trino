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

import com.google.inject.Inject;
import io.starburst.stargate.accesscontrol.client.ColumnMaskExpression;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.FunctionId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.TableId;
import io.trino.server.galaxy.GalaxyPermissionsCache;
import io.trino.server.galaxy.GalaxyPermissionsCache.GalaxyQueryPermissions;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.ViewExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.security.galaxy.GalaxyIdentity.getContextRoleId;
import static io.trino.server.security.galaxy.GalaxyIdentity.getRowFilterAndColumnMaskUserString;
import static io.trino.server.security.galaxy.GalaxyIdentity.toDispatchSession;
import static java.util.Objects.requireNonNull;

public class GalaxySystemAccessController
{
    private final TrinoSecurityApi accessControlClient;
    private final CatalogIds catalogIds;

    private final GalaxyPermissionsCache galaxyPermissionsCache;

    @Inject
    public GalaxySystemAccessController(TrinoSecurityApi accessControlClient, CatalogIds catalogIds, GalaxyPermissionsCache galaxyPermissionsCache)
    {
        this.accessControlClient = requireNonNull(accessControlClient, "accessControlClient is null");
        this.catalogIds = requireNonNull(catalogIds, "catalogIds is null");
        this.galaxyPermissionsCache = requireNonNull(galaxyPermissionsCache, "galaxyPermissionsCache is null");
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
        return withGalaxyPermissions(context, permissions -> permissions.getEntityPrivileges(getContextRoleId(context), entity, accessControlClient));
    }

    public Map<RoleName, RoleId> listEnabledRoles(SystemSecurityContext context)
    {
        return withGalaxyPermissions(context, permissions -> permissions.listEnabledRoles(accessControlClient));
    }

    public Predicate<String> getCatalogVisibility(SystemSecurityContext context)
    {
        return withGalaxyPermissions(context, permissions -> {
            ContentsVisibility catalogVisibility = permissions.getCatalogVisibility(accessControlClient);
            return catalogName -> catalogIds.getCatalogId(catalogName)
                    .map(CatalogId::toString)
                    .map(catalogVisibility::isVisible)
                    .orElse(false);
        });
    }

    public AccountId getAccountId(SystemSecurityContext context)
    {
        return toDispatchSession(context.getIdentity()).getAccountId();
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
        return withGalaxyPermissions(context, permissions -> permissions.getRoleDisplayName(roleId, accessControlClient));
    }

    public boolean canExecuteFunction(SystemSecurityContext context, FunctionId functionId)
    {
        return accessControlClient.canExecuteFunction(toDispatchSession(context.getIdentity()), functionId);
    }

    public List<ViewExpression> getRowFilters(SystemSecurityContext context, TableId tableId)
    {
        return getEntityPrivileges(context, tableId).getRowFilters().stream()
                .map(filter -> new ViewExpression(
                        getRowFilterAndColumnMaskUserString(context.getIdentity(), filter.owningRoleId()),
                        catalogIds.getCatalogName(tableId.getCatalogId()),
                        Optional.of(tableId.getSchemaName()),
                        filter.expression()))
                .collect(toImmutableList());
    }

    /**
     * Return the ViewExpression for the column mask corresponding to the columnName,
     * or Optional.empty() if none exists.  If the specific columnName isn't found,
     * look up the wildcard columnName "*".  Right now Trino supports at most one
     * column mask for any column.
     */
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, String columnName, TableId tableId)
    {
        Map<String, ColumnMaskExpression> masks = getEntityPrivileges(context, tableId).getColumnMasks();

        // Use the mask for the column name if it exists, otherwise look for
        // the mask for the wildcard column name
        ColumnMaskExpression columnMask = masks.getOrDefault(columnName, masks.get("*"));
        if (columnMask == null) {
            // No columnMask matches, so return empty
            return Optional.empty();
        }

        return Optional.of(new ViewExpression(
                getRowFilterAndColumnMaskUserString(context.getIdentity(), columnMask.owningRoleId()),
                catalogIds.getCatalogName(tableId.getCatalogId()),
                Optional.of(tableId.getSchemaName()),
                columnMask.expression()));
    }

    private <V> V withGalaxyPermissions(SystemSecurityContext context, Function<GalaxyQueryPermissions, V> permissionsFunction)
    {
        return galaxyPermissionsCache.withGalaxyPermissions(toDispatchSession(context.getIdentity()), context.getQueryId(), permissionsFunction);
    }
}
