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
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionDetails;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.FunctionId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.server.galaxy.GalaxyPermissionsCache;
import io.trino.server.galaxy.GalaxyPermissionsCache.GalaxyQueryPermissions;
import io.trino.server.galaxy.catalogs.CatalogResolver;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.ViewExpression;
import io.trino.transaction.TransactionId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.server.security.galaxy.GalaxyIdentity.getContextRoleId;
import static io.trino.server.security.galaxy.GalaxyIdentity.getRowFilterAndColumnMaskUserString;
import static io.trino.server.security.galaxy.GalaxyIdentity.toDispatchSession;
import static java.util.Objects.requireNonNull;

public class GalaxySystemAccessController
{
    private final TrinoSecurityApi accessControlClient;
    private final CatalogResolver catalogResolver;
    private final GalaxyPermissionsCache galaxyPermissionsCache;
    private final Optional<TransactionId> transactionId;

    @Inject
    public GalaxySystemAccessController(TrinoSecurityApi accessControlClient, CatalogResolver catalogResolver, GalaxyPermissionsCache galaxyPermissionsCache)
    {
        this(accessControlClient, catalogResolver, galaxyPermissionsCache, Optional.empty());
    }

    public GalaxySystemAccessController(TrinoSecurityApi accessControlClient, CatalogResolver catalogResolver, GalaxyPermissionsCache galaxyPermissionsCache, Optional<TransactionId> transactionId)
    {
        this.accessControlClient = requireNonNull(accessControlClient, "accessControlClient is null");
        this.catalogResolver = requireNonNull(catalogResolver, "catalogResolver is null");
        this.galaxyPermissionsCache = requireNonNull(galaxyPermissionsCache, "galaxyPermissionsCache is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
    }

    public Optional<CatalogId> getCatalogId(String catalogName)
    {
        return catalogResolver.getCatalogId(transactionId, catalogName);
    }

    public boolean isReadOnlyCatalog(String catalogName)
    {
        return catalogResolver.isReadOnlyCatalog(transactionId, catalogName);
    }

    public Optional<SharedSchemaNameAndAccepted> getSharedCatalogSchemaName(String catalogName)
    {
        return catalogResolver.getSharedSchemaForCatalog(transactionId, catalogName);
    }

    /**
     * @see #getEntityPrivileges(Identity, EntityId)
     */
    public EntityPrivileges getEntityPrivileges(SystemSecurityContext context, EntityId entity)
    {
        return getCache(context).getEntityPrivileges(getContextRoleId(context.getIdentity()), entity);
    }

    /**
     * Equivalent of {@link #getEntityPrivileges(SystemSecurityContext, EntityId)} but without caching.
     * To be used when caching is not possible due to lack of query ID.
     *
     * @see #getEntityPrivileges(SystemSecurityContext, EntityId)
     */
    public EntityPrivileges getEntityPrivileges(Identity identity, EntityId entity)
    {
        return accessControlClient.getEntityPrivileges(toDispatchSession(identity), getContextRoleId(identity), entity);
    }

    public Map<RoleName, RoleId> listEnabledRoles(Identity identity)
    {
        return listEnabledRoles(identity, GalaxyIdentity::toDispatchSession);
    }

    public Map<RoleName, RoleId> listEnabledRoles(Identity identity, Function<Identity, DispatchSession> sessionCreator)
    {
        return accessControlClient.listEnabledRoles(sessionCreator.apply(identity));
    }

    public Predicate<String> getCatalogVisibility(SystemSecurityContext context, Set<String> requestedCatalogs)
    {
        Set<CatalogId> requestedCatalogIds = requestedCatalogs.stream()
                .flatMap(catalogName -> catalogResolver.getCatalogId(transactionId, catalogName).stream())
                .collect(toImmutableSet());
        Predicate<CatalogId> catalogVisibility = getCache(context).getCatalogVisibility(requestedCatalogIds);
        return catalogName -> {
            checkArgument(requestedCatalogs.contains(catalogName), "Unexpected catalog checked for visibility: %s, expected one of: %s", catalogName, requestedCatalogs);
            return catalogResolver.getCatalogId(transactionId, catalogName)
                    .map(catalogVisibility::test)
                    .orElse(false);
        };
    }

    public void implyCatalogVisibility(SystemSecurityContext context, String catalogName)
    {
        getCache(context).implyCatalogVisibility(catalogName);
    }

    public boolean hasImpliedCatalogVisibility(SystemSecurityContext context, String catalogName)
    {
        return getCache(context).hasImpliedCatalogVisibility(catalogName);
    }

    public AccountId getAccountId(Identity identity)
    {
        return toDispatchSession(identity).getAccountId();
    }

    public Predicate<String> getVisibilityForSchemas(SystemSecurityContext context, CatalogId catalogId, Set<String> schemaNames)
    {
        // This is only called once per query, so no need to cache  TODO: I wonder if this is true?
        checkArgument(!schemaNames.contains("information_schema"), "Unexpected schema names: %s", schemaNames);
        if (schemaNames.isEmpty()) {
            return schemaName -> {
                throw new UnsupportedOperationException("Cannot provide visibility for schema when no schema names were provided: " + schemaName);
            };
        }
        ContentsVisibility visibility = accessControlClient.getVisibilityForSchemas(toDispatchSession(context.getIdentity()), catalogId, schemaNames);
        return schemaName -> {
            checkArgument(schemaNames.contains(schemaName), "Invalid schema name consulted in predicate constructed for %s: %s", schemaNames, schemaName);
            return visibility.isVisible(schemaName);
        };
    }

    public Predicate<SchemaTableName> getTableVisibility(SystemSecurityContext context, CatalogId catalogId, Set<String> schemaNames)
    {
        Map<String, ContentsVisibility> tableVisibility = getCache(context).getTableVisibility(catalogId, schemaNames);
        return name -> {
            ContentsVisibility contentsVisibility = tableVisibility.get(name.getSchemaName());
            return contentsVisibility != null && contentsVisibility.isVisible(name.getTableName());
        };
    }

    public ContentsVisibility getVisibilityForTables(SystemSecurityContext context, CatalogId catalogId, String schemaName, Set<String> tableNames)
    {
        return getCache(context).getVisibilityForTables(catalogId, schemaName, tableNames);
    }

    public String getRoleDisplayName(Identity identity, RoleId roleId)
    {
        // Not cached because this is used for error messages only, so at most once per query.
        return accessControlClient.listRoles(toDispatchSession(identity)).entrySet().stream()
                .filter(entry -> roleId.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .findFirst()
                .map(RoleName::toString)
                .orElse(roleId.toString());
    }

    public boolean canExecuteFunction(SystemSecurityContext context, FunctionId functionId)
    {
        return accessControlClient.canExecuteFunction(toDispatchSession(context.getIdentity()), functionId);
    }

    public boolean canExecuteUserDefinedFunction(SystemSecurityContext context, FunctionId functionId)
    {
        return getAvailableFunctions(context).stream().anyMatch(function -> functionId.equals(function.functionId()));
    }

    public List<ViewExpression> getRowFilters(SystemSecurityContext context, TableId tableId)
    {
        return getEntityPrivileges(context, tableId).getRowFilters().stream()
                .map(filter -> {
                    ViewExpression.Builder builder = ViewExpression.builder();

                    getRowFilterAndColumnMaskUserString(context.getIdentity(), filter.owningRoleId()).ifPresent(builder::identity);
                    catalogResolver.getCatalogName(transactionId, tableId.getCatalogId()).ifPresent(builder::catalog);

                    return builder.schema(tableId.getSchemaName())
                            .expression(filter.expression())
                            .build();
                })
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

        ViewExpression.Builder builder = ViewExpression.builder();

        getRowFilterAndColumnMaskUserString(context.getIdentity(), columnMask.owningRoleId()).ifPresent(builder::identity);
        catalogResolver.getCatalogName(transactionId, tableId.getCatalogId()).ifPresent(builder::catalog);

        return Optional.of(builder.schema(tableId.getSchemaName())
                .expression(columnMask.expression())
                .build());
    }

    public Set<GalaxyLanguageFunctionDetails> getAvailableFunctions(SystemSecurityContext context)
    {
        return getCache(context).getAvailableFunctions(getContextRoleId(context.getIdentity()));
    }

    private GalaxyQueryPermissions getCache(SystemSecurityContext context)
    {
        return galaxyPermissionsCache.getCache(accessControlClient, context.getQueryId(), toDispatchSession(context.getIdentity()));
    }
}
