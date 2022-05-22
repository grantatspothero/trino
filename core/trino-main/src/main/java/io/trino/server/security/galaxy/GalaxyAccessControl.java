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

import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GalaxyPrivilegeInfo;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.EntityKind;
import io.starburst.stargate.id.FunctionId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.SchemaId;
import io.starburst.stargate.id.TableId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_CATALOG;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_SCHEMA;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_TABLE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.DELETE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.EXECUTE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.INSERT;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.UPDATE;
import static io.trino.server.security.galaxy.GalaxyIdentity.getRoleId;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.AccessDeniedException.denyAddColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDropColumn;
import static io.trino.spi.security.AccessDeniedException.denyDropMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyDropView;
import static io.trino.spi.security.AccessDeniedException.denyExecuteFunction;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denyInsertTable;
import static io.trino.spi.security.AccessDeniedException.denyRefreshMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameColumn;
import static io.trino.spi.security.AccessDeniedException.denyRenameMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denySelectColumns;
import static io.trino.spi.security.AccessDeniedException.denySetMaterializedViewProperties;
import static io.trino.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetTableAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetTableProperties;
import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;
import static io.trino.spi.security.AccessDeniedException.denyShowColumns;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyTruncateTable;
import static io.trino.spi.security.AccessDeniedException.denyUpdateTableColumns;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class GalaxyAccessControl
        implements SystemAccessControl
{
    public static final String NAME = "galaxy";

    private final GalaxyAccessControllerSupplier controllerSupplier;

    public GalaxyAccessControl(GalaxyAccessControllerSupplier controllerSupplier)
    {
        this.controllerSupplier = requireNonNull(controllerSupplier, "controllerSupplier is null");
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support user impersonation");
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        // Allow - this is a deprecated check that must be allowed, or all queries will fail
    }

    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context)
    {
        // Allow - cluster access is enforced in the dispatcher
    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, Identity queryOwner)
    {
        checkRoleOwnsQuery(context, queryOwner, AccessDeniedException::denyViewQuery);
    }

    @Override
    public Collection<Identity> filterViewQuery(SystemSecurityContext context, Collection<Identity> queryOwners)
    {
        return filterIdentitiesByActiveRoleSet(context, queryOwners);
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(SystemSecurityContext context, Collection<Identity> queryOwners)
    {
        return filterIdentitiesByActiveRoleSet(context, queryOwners);
    }

    @Override
    public void checkCanKillQueryOwnedBy(SystemSecurityContext context, Identity queryOwner)
    {
        checkRoleOwnsQuery(context, queryOwner, AccessDeniedException::denyKillQuery);
    }

    @Override
    public void checkCanReadSystemInformation(SystemSecurityContext context)
    {
        throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support directly reading Trino cluster system information");
    }

    @Override
    public void checkCanWriteSystemInformation(SystemSecurityContext context)
    {
        throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support directly writing Trino cluster system information");
    }

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
    {
        // Allow - all session properties are currently allowed
    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        // Allow - the granular check in this class are called after this check
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        Predicate<String> catalogVisibility = controllerSupplier.apply(context).getCatalogVisibility(context);
        return catalogs.stream()
                .filter(catalog -> isSystemCatalog(catalog) || catalogVisibility.test(catalog))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        checkCatalogIsWritable(controllerSupplier.apply(context), schema.getCatalogName(), explanation -> denyCreateSchema(schema.toString(), explanation));
        checkHasCatalogPrivilege(context, schema.getCatalogName(), CREATE_SCHEMA, explanation -> denyCreateSchema(schema.toString(), explanation));
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        checkCatalogWritableAndSchemaOwner(context, schema, explanation -> denyDropSchema(schema.toString(), explanation));
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        checkCatalogWritableAndSchemaOwner(context, schema, explanation -> denyRenameSchema(schema.toString(), newSchemaName, explanation));
        checkHasCatalogPrivilege(context, schema.getCatalogName(), CREATE_SCHEMA, explanation ->
                denyRenameSchema(schema.toString(), newSchemaName, explanation));
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        checkCatalogWritableAndSchemaOwner(context, schema, explanation -> denySetSchemaAuthorization(schema.toString(), principal, explanation));
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        // Allow - schemas are filtered based on visibility
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        if (isSystemCatalog(catalogName)) {
            return schemaNames;
        }

        Predicate<String> schemaVisibility = getSchemaVisibility(context, catalogName);
        return schemaNames.stream()
                .filter(name -> isInformationSchema(name) || schemaVisibility.test(name))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        if (!isSystemCatalog(schemaName) && !isSchemaVisible(context, schemaName)) {
            denyShowCreateSchema(schemaName.toString(), entityIsNotVisible(context, "Schema", schemaName.toString()));
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!isSystemCatalog(table) && !isTableVisible(context, table)) {
            denyShowCreateTable(table.toString(), entityIsNotVisible(context, "Table", table.toString()));
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        checkCatalogIsWritable(controllerSupplier.apply(context), table.getCatalogName(), explanation -> denyCreateTable(table.toString(), explanation));
        checkHasSchemaPrivilege(context, table, CREATE_TABLE, explanation -> denyCreateTable(table.toString(), explanation));
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkCatalogWritableAndTableOwner(context, table, explanation -> denyDropTable(table.toString(), explanation));
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        checkCatalogWritableAndTableOwner(context, table, explanation -> denyRenameTable(table.toString(), explanation));
        checkHasSchemaPrivilege(context, table, CREATE_TABLE, explanation -> denyRenameTable(table.toString(), explanation));
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        checkCatalogWritableAndTableOwner(context, table, explanation -> denySetTableProperties(table.toString(), explanation));
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkCatalogWritableAndTableOwner(context, table, explanation -> denyCommentTable(table.toString(), explanation));
    }

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        checkCatalogWritableAndViewOwner(context, view, explanation -> AccessDeniedException.denySetViewComment(view.toString(), explanation));
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkCatalogWritableAndTableOwner(context, table, explanation -> denyCommentColumn(table.toString(), explanation));
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        // Allow - tables are filtered based on visibility
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        if (isSystemCatalog(catalogName)) {
            return tableNames;
        }

        Set<String> schemaNames = tableNames.stream()
                .map(SchemaTableName::getSchemaName)
                .filter(schema -> !isInformationSchema(schema))
                .collect(toImmutableSet());

        Predicate<SchemaTableName> tableVisibility = getTableVisibility(context, catalogName, schemaNames);
        return tableNames.stream()
                .filter(name -> isInformationSchema(name.getSchemaName()) || tableVisibility.test(name))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isSystemCatalog(table.getCatalogName())) {
            return;
        }
        GalaxySystemAccessController controller = controllerSupplier.apply(context);
        EntityPrivileges privileges = controller.getEntityPrivileges(context, toTableId(controller, table).orElseThrow(() -> new TableNotFoundException(table.getSchemaTableName())));

        // Check that the user has wildcard SELECT column privilege
        ContentsVisibility visibility = privileges.getColumnPrivileges().get(SELECT.name());
        if (visibility != null && visibility.defaultVisibility() == GrantKind.ALLOW && visibility.overrideVisibility().isEmpty()) {
            return;
        }
        denyShowColumns(table.toString(), roleLacksPrivilege(context, SELECT, "all columns of", table.toString()));
    }

    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (isSystemCatalog(table.getCatalogName()) || isInformationSchema(table.getSchemaTableName().getSchemaName())) {
            return columns;
        }
        GalaxySystemAccessController controller = controllerSupplier.apply(context);
        EntityPrivileges privileges = controller.getEntityPrivileges(context, toTableId(controller, table).orElseThrow(() -> new TableNotFoundException(table.getSchemaTableName())));
        ContentsVisibility visibility = privileges.getColumnPrivileges().get(SELECT.name());
        if (visibility == null) {
            visibility = privileges.getColumnPrivileges().get("VISIBLE");
            if (visibility == null) {
                return ImmutableSet.of();
            }
        }

        return columns.stream()
                .filter(visibility::isVisible)
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkCatalogWritableAndTableOwner(context, table, explanation -> denyAddColumn(table.toString(), explanation));
    }

    @Override
    public void checkCanAlterColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkCatalogWritableAndTableOwner(context, table, explanation -> denyAddColumn(table.toString(), explanation));
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkCatalogWritableAndTableOwner(context, table, explanation -> denyDropColumn(table.toString(), explanation));
    }

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        checkCatalogWritableAndTableOwner(context, table, explanation -> denySetTableAuthorization(table.toString(), principal, explanation));
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkCatalogWritableAndTableOwner(context, table, explanation -> denyRenameColumn(table.toString(), explanation));
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (isSchemaDiscovery(table)) {
            checkHasAccountPrivilege(context, CREATE_CATALOG, explanation -> denySelectColumns(table.toString(), columns, explanation));
        }
        else if (!isSystemCatalog(table)) {
            checkHasPrivilegeOnColumns(context, SELECT, false, table, columns, explanation -> denySelectColumns(table.toString(), columns, explanation));
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkCatalogWritableAndHasTablePrivilege(context, table, INSERT, explanation -> denyInsertTable(table.toString(), explanation));
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkCatalogWritableAndHasTablePrivilege(context, table, DELETE, explanation -> denyDeleteTable(table.toString(), explanation));
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        checkCatalogWritableAndHasTablePrivilege(context, table, DELETE, explanation -> denyTruncateTable(table.toString(), explanation));
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        checkCatalogIsWritable(controllerSupplier.apply(context), table.getCatalogName(), explanation -> denyUpdateTableColumns(table.toString(), columns, explanation));
        checkHasPrivilegeOnColumns(context, UPDATE, false, table, columns, explanation -> denyUpdateTableColumns(table.toString(), columns, explanation));
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        checkCatalogIsWritable(controllerSupplier.apply(context), view.getCatalogName(), explanation -> denyCreateView(view.toString(), explanation));
        checkHasSchemaPrivilege(context, view, CREATE_TABLE, explanation -> denyCreateView(view.toString(), explanation));
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        Consumer<String> denier = explanation -> denyRenameView(view.toString(), newView.toString(), explanation);
        checkCatalogWritableAndViewOwner(context, view, denier);
        checkCatalogIsWritable(controllerSupplier.apply(context), newView.getCatalogName(), denier);
        checkHasSchemaPrivilege(context, newView, CREATE_TABLE, denier);
    }

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        checkCatalogWritableAndViewOwner(context, view, explanation -> denySetViewAuthorization(view.toString(), principal, explanation));
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        checkCatalogWritableAndViewOwner(context, view, explanation -> denyDropView(view.toString(), explanation));
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        checkHasPrivilegeOnColumns(context, SELECT, true, table, columns, explanation ->
                denyCreateViewWithSelect(table.toString(), context.getIdentity().toConnectorIdentity(), explanation));
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        checkCatalogIsWritable(controllerSupplier.apply(context), materializedView.getCatalogName(), explanation -> denyCreateMaterializedView(materializedView.toString(), explanation));
        checkHasSchemaPrivilege(context, materializedView, CREATE_TABLE, explanation -> denyCreateMaterializedView(materializedView.toString(), explanation));
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        checkCatalogWritableAndMaterializedViewOwner(context, materializedView, explanation -> denyRefreshMaterializedView(materializedView.toString(), explanation));
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        checkCatalogWritableAndMaterializedViewOwner(context, materializedView, explanation -> denySetMaterializedViewProperties(materializedView.toString(), explanation));
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        checkCatalogWritableAndMaterializedViewOwner(context, materializedView, explanation -> denyDropMaterializedView(materializedView.toString(), explanation));
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, CatalogSchemaTableName newView)
    {
        checkCatalogWritableAndMaterializedViewOwner(context, materializedView, explanation -> denyRenameMaterializedView(materializedView.toString(), newView.toString(), explanation));
        checkCatalogIsWritable(controllerSupplier.apply(context), newView.getCatalogName(), explanation -> denyRenameMaterializedView(materializedView.toString(), newView.toString(), explanation));
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, TrinoPrincipal grantee, boolean grantOption)
    {
        // Not allowed unless the identity user is a view definer user
        if (!context.getIdentity().getUser().startsWith("<galaxy role ")) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support grants on functions");
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        // Allow - all session properties are currently allowed
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(SystemSecurityContext context)
    {
        // Showing role grants is safe because galaxy filters the grants to what is visible
    }

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context)
    {
        // Galaxy does the checking
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext context, CatalogSchemaRoutineName procedure)
    {
        // Allow - procedures have internal security checks
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext context, String functionName)
    {
        // Allow - all functions are public and allowed
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext context, FunctionKind functionKind, CatalogSchemaRoutineName function)
    {
        if (functionKind != FunctionKind.TABLE) {
            denyExecuteFunction(function.toString(), "Function is of type %s and only TABLE functions are supported".formatted(functionKind));
        }
        GalaxySystemAccessController controller = controllerSupplier.apply(context);
        Optional<CatalogId> catalogId = controller.getCatalogId(function.getCatalogName());
        if (catalogId.isEmpty()) {
            denyExecuteFunction(function.toString(), "Could not find catalog: %s".formatted(function.getCatalogName()));
        }
        if (!controller.canExecuteFunction(context, new FunctionId(catalogId.get(), function.getSchemaName(), function.getRoutineName()))) {
            denyExecuteFunction(function.toString(), roleLacksPrivilege(context, EXECUTE, "function", function.toString()));
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext context, CatalogSchemaTableName table, String procedure)
    {
        if (isSystemCatalog(table.getCatalogName())) {
            denyExecuteProcedure(procedure, "Table procedures are not permitted on the system catalog");
        }
        checkIsTableOwner(context, table, explanation -> denyExecuteProcedure(procedure, explanation));
    }

    // Helper methods that provide explanations for denied access

    private void checkRoleOwnsQuery(SystemSecurityContext context, Identity queryOwner, Consumer<String> denier)
    {
        if (!controllerSupplier.apply(context).listEnabledRoles(context).containsValue(getRoleId(queryOwner))) {
            denier.accept(roleIsNotQueryOwner(context, queryOwner));
        }
    }

    private void checkCatalogIsWritable(GalaxySystemAccessController controller, String catalogName, Consumer<String> denier)
    {
        if (isReadOnlyCatalog(controller, catalogName)) {
            denier.accept(catalogIsReadOnly(catalogName));
        }
    }

    private void checkHasCatalogPrivilege(SystemSecurityContext context, String catalogName, Privilege privilege, Consumer<String> denier)
    {
        if (!hasEntityPrivilege(context, controllerSupplier.apply(context).getCatalogId(catalogName), privilege, false)) {
            denier.accept(roleLacksPrivilege(context, privilege, "catalog", catalogName));
        }
    }

    private void checkHasSchemaPrivilege(SystemSecurityContext context, CatalogSchemaTableName table, Privilege privilege, Consumer<String> denier)
    {
        if (!hasEntityPrivilege(context, toSchemaId(controllerSupplier.apply(context), table), privilege, false)) {
            denier.accept(roleLacksPrivilege(context, privilege, "schema", new CatalogSchemaName(table.getCatalogName(), table.getSchemaTableName().getSchemaName()).toString()));
        }
    }

    private void checkHasTablePrivilege(SystemSecurityContext context, CatalogSchemaTableName table, Privilege privilege, Consumer<String> denier)
    {
        Optional<TableId> tableId = toTableId(controllerSupplier.apply(context), table);
        if (!hasEntityPrivilege(context, tableId, privilege, false)) {
            denier.accept(roleLacksPrivilege(context, privilege, "table", table.toString()));
        }
    }

    private void checkHasPrivilegeOnColumns(SystemSecurityContext context, Privilege privilege, boolean requiresGrantOption, CatalogSchemaTableName table, Set<String> columns, Consumer<String> denier)
    {
        GalaxySystemAccessController controller = controllerSupplier.apply(context);
        Optional<TableId> tableId = toTableId(controller, table);
        if (tableId.isEmpty()) {
            runPrivilegeDenier(context, privilege, columns, denier);
        }
        EntityPrivileges privileges = controller.getEntityPrivileges(context, tableId.get());
        if (requiresGrantOption) {
            Optional<GalaxyPrivilegeInfo> info = privileges.getPrivileges().stream().filter(element -> element.getPrivilege() == privilege && element.isGrantOption()).findAny();
            if (info.isEmpty() || !info.get().isGrantOption()) {
                runPrivilegeDenier(context, privilege, columns, denier);
            }
        }
        ContentsVisibility visibility = privileges.getColumnPrivileges().get(privilege.name());
        Set<String> deniedColumns;
        if (visibility == null) {
            deniedColumns = columns;
        }
        else {
            deniedColumns = columns.stream().filter(column -> !visibility.isVisible(column)).collect(toImmutableSet());
        }
        if (!deniedColumns.isEmpty()) {
            runPrivilegeDenier(context, privilege, deniedColumns, denier);
        }
    }

    private void runPrivilegeDenier(SystemSecurityContext context, Privilege privilege, Set<String> deniedColumns, Consumer<String> denier)
    {
        String kind = deniedColumns.size() > 1 ? "columns" : "column";
        denier.accept(roleLacksPrivilege(context, privilege, kind, "[%s]".formatted(String.join(", ", deniedColumns))));
    }

    private void checkHasAccountPrivilege(SystemSecurityContext context, Privilege privilege, Consumer<String> denier)
    {
        AccountId accountId = controllerSupplier.apply(context).getAccountId(context);
        if (!hasEntityPrivilege(context, Optional.of(accountId), privilege, false)) {
            denier.accept(roleLacksPrivilege(context, privilege, "account", accountId.toString()));
        }
    }

    private void checkCatalogWritableAndSchemaOwner(SystemSecurityContext context, CatalogSchemaName schema, Consumer<String> denier)
    {
        checkCatalogIsWritable(controllerSupplier.apply(context), schema.getCatalogName(), denier);
        checkIsSchemaOwner(context, schema, denier);
    }

    private void checkCatalogWritableAndTableOwner(SystemSecurityContext context, CatalogSchemaTableName table, Consumer<String> denier)
    {
        checkCatalogIsWritable(controllerSupplier.apply(context), table.getCatalogName(), denier);
        checkIsTableOwner(context, table, denier);
    }

    public void checkCatalogWritableAndViewOwner(SystemSecurityContext context, CatalogSchemaTableName view, Consumer<String> denier)
    {
        checkCatalogIsWritable(controllerSupplier.apply(context), view.getCatalogName(), denier);
        checkIsViewOwner(context, view, denier);
    }

    public void checkCatalogWritableAndMaterializedViewOwner(SystemSecurityContext context, CatalogSchemaTableName view, Consumer<String> denier)
    {
        checkCatalogIsWritable(controllerSupplier.apply(context), view.getCatalogName(), denier);
        if (!isTableOwner(context, view)) {
            denier.accept(format("Role %s does not own the materialized view", currentRoleName(context)));
        }
    }

    private void checkCatalogWritableAndHasTablePrivilege(SystemSecurityContext context, CatalogSchemaTableName table, Privilege privilege, Consumer<String> denier)
    {
        checkCatalogIsWritable(controllerSupplier.apply(context), table.getCatalogName(), denier);
        checkHasTablePrivilege(context, table, privilege, denier);
    }

    private void checkIsSchemaOwner(SystemSecurityContext context, CatalogSchemaName schema, Consumer<String> denier)
    {
        if (!isSchemaOwner(context, schema)) {
            denier.accept(format("Role %s does not own the schema", currentRoleName(context)));
        }
    }

    private void checkIsTableOwner(SystemSecurityContext context, CatalogSchemaTableName table, Consumer<String> denier)
    {
        if (!isTableOwner(context, table)) {
            denier.accept(format("Role %s does not own the table", currentRoleName(context)));
        }
    }

    private void checkIsViewOwner(SystemSecurityContext context, CatalogSchemaTableName view, Consumer<String> denier)
    {
        if (!isTableOwner(context, view)) {
            denier.accept(format("Role %s does not own the view", currentRoleName(context)));
        }
    }

    private String entityIsNotVisible(SystemSecurityContext context, String description, String entityName)
    {
        return format("%s %s is not visible to the role %s", entityName, description, currentRoleName(context));
    }

    private String roleIsNotQueryOwner(SystemSecurityContext context, Identity queryOwner)
    {
        return format(
                "Role %s does not own the query%s",
                controllerSupplier.apply(context).getRoleDisplayName(context, getRoleId(queryOwner)),
                context.getQueryId().map(queryId -> " " + queryId).orElse(""));
    }

    private String catalogIsReadOnly(String catalog)
    {
        return format("Catalog %s only allows read-only access", catalog);
    }

    private String roleLacksPrivilege(SystemSecurityContext context, Privilege privilege, String kind, String entity)
    {
        return format("Role %s does not have the privilege %s on the %s %s", currentRoleName(context), privilege, kind, entity);
    }

    // Helper methods that call Galaxy to determine access

    private boolean hasEntityPrivilege(SystemSecurityContext context, Optional<? extends EntityId> entity, Privilege privilege, boolean requiresGrantOption)
    {
        if (entity.isEmpty()) {
            return false;
        }

        EntityId entityId = entity.get();
        EntityPrivileges entityPrivileges = controllerSupplier.apply(context).getEntityPrivileges(context, entityId);
        boolean privilegeMatch = entityPrivileges.getPrivileges().stream()
                .filter(privilegeInfo -> privilegeInfo.getPrivilege() == privilege)
                .anyMatch(privilegeInfo -> !requiresGrantOption || privilegeInfo.isGrantOption());
        if (!privilegeMatch) {
            return false;
        }
        if (entityId.getEntityKind() != EntityKind.TABLE) {
            return true;
        }

        // Check the column privileges
        ContentsVisibility visibility = requireNonNull(entityPrivileges.getColumnPrivileges().get(privilege.name()), "column visibility for privilege is null");
        return visibility.defaultVisibility() == GrantKind.ALLOW;
    }

    private boolean isSchemaVisible(SystemSecurityContext context, CatalogSchemaName schema)
    {
        return getSchemaVisibility(context, schema.getCatalogName()).test(schema.getSchemaName());
    }

    private Predicate<String> getSchemaVisibility(SystemSecurityContext context, String catalogName)
    {
        Optional<CatalogId> catalogId = controllerSupplier.apply(context).getCatalogId(catalogName);
        if (catalogId.isEmpty()) {
            return ignored -> false;
        }
        return controllerSupplier.apply(context).getSchemaVisibility(context, catalogId.get());
    }

    private List<Identity> filterIdentitiesByActiveRoleSet(SystemSecurityContext context, Collection<Identity> identities)
    {
        Set<RoleId> enabledRoles = ImmutableSet.copyOf(controllerSupplier.apply(context).listEnabledRoles(context).values());
        return identities.stream()
                .filter(identity -> enabledRoles.contains(getRoleId(identity)))
                .collect(toImmutableList());
    }

    private boolean isSchemaOwner(SystemSecurityContext context, CatalogSchemaName schema)
    {
        GalaxySystemAccessController controller = controllerSupplier.apply(context);
        return isEntityOwner(controller, context, toSchemaId(controller, schema));
    }

    private boolean isTableOwner(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        GalaxySystemAccessController controller = controllerSupplier.apply(context);
        return isEntityOwner(controller, context, toTableId(controller, tableName));
    }

    private boolean isTableVisible(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        return getTableVisibility(context, tableName.getCatalogName(), ImmutableSet.of(tableName.getSchemaTableName().getSchemaName()))
                .test(tableName.getSchemaTableName());
    }

    private Predicate<SchemaTableName> getTableVisibility(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        Optional<CatalogId> catalogId = controllerSupplier.apply(context).getCatalogId(catalogName);
        if (catalogId.isEmpty()) {
            return ignored -> false;
        }
        return controllerSupplier.apply(context).getTableVisibility(context, catalogId.get(), schemaNames);
    }

    private boolean isEntityOwner(GalaxySystemAccessController controller, SystemSecurityContext context, Optional<? extends EntityId> entity)
    {
        if (entity.isEmpty()) {
            return false;
        }

        String owner = controller.getEntityPrivileges(context, entity.get()).getOwner().getName();
        return context.getIdentity().getEnabledRoles().contains(owner);
    }

    private Optional<SchemaId> toSchemaId(GalaxySystemAccessController controller, CatalogSchemaName schema)
    {
        return controller.getCatalogId(schema.getCatalogName())
                .map(catalogId -> new SchemaId(catalogId, schema.getSchemaName()));
    }

    private Optional<SchemaId> toSchemaId(GalaxySystemAccessController controller, CatalogSchemaTableName table)
    {
        return controller.getCatalogId(table.getCatalogName())
                .map(catalogId -> new SchemaId(catalogId, table.getSchemaTableName().getSchemaName()));
    }

    private Optional<TableId> toTableId(GalaxySystemAccessController controller, CatalogSchemaTableName table)
    {
        return controller.getCatalogId(table.getCatalogName())
                .map(catalogId -> new TableId(catalogId, table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName()));
    }

    private boolean isReadOnlyCatalog(GalaxySystemAccessController controller, String name)
    {
        return isSystemCatalog(name) || isInformationSchema(name) || controller.isReadOnlyCatalog(name);
    }

    private String currentRoleName(SystemSecurityContext context)
    {
        return controllerSupplier.apply(context).getRoleDisplayName(context, getRoleId(context.getIdentity()));
    }

    private static boolean isSystemCatalog(String name)
    {
        return "system".equals(name);
    }

    private static boolean isInformationSchema(String name)
    {
        return "information_schema".equals(name);
    }

    private static boolean isSchemaDiscovery(CatalogSchemaTableName table)
    {
        return table.getSchemaTableName().equals(new SchemaTableName("schema_discovery", "discovery"));
    }

    private static boolean isSystemCatalog(CatalogSchemaName name)
    {
        return isSystemCatalog(name.getCatalogName()) || isInformationSchema(name.getSchemaName());
    }

    private static boolean isSystemCatalog(CatalogSchemaTableName name)
    {
        return isSystemCatalog(name.getCatalogName()) || isInformationSchema(name.getSchemaTableName().getSchemaName());
    }
}
