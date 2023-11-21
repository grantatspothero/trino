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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
import io.trino.security.FullSystemSecurityContext;
import io.trino.server.security.galaxy.GalaxySystemAccessControlConfig.FilterColumnsAcceleration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;
import jakarta.ws.rs.NotFoundException;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.starburst.stargate.accesscontrol.client.OperationNotAllowedException.operationNotAllowed;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_FUNCTION;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_SCHEMA;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_TABLE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.DELETE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.INSERT;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.UPDATE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.VIEW_ALL_QUERY_HISTORY;
import static io.trino.execution.PrivilegeUtilities.getPrivilegesForEntityKind;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.plugin.base.util.Parallels.processWithAdditionalThreads;
import static io.trino.server.security.galaxy.GalaxyIdentity.getContextRoleId;
import static io.trino.server.security.galaxy.GalaxyIdentity.getRoleId;
import static io.trino.server.security.galaxy.GalaxySecuritySessionProperties.getFilterColumnsAcceleration;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.AccessDeniedException.denyAddColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateCatalog;
import static io.trino.spi.security.AccessDeniedException.denyCreateFunction;
import static io.trino.spi.security.AccessDeniedException.denyCreateMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDropCatalog;
import static io.trino.spi.security.AccessDeniedException.denyDropColumn;
import static io.trino.spi.security.AccessDeniedException.denyDropFunction;
import static io.trino.spi.security.AccessDeniedException.denyDropMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyDropView;
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

    private static final int MAX_OUTSTANDING_BACKGROUND_TASKS = 8192;
    private static final Pattern DML_OPERATION_MATCHER = Pattern.compile("(select|insert|update|delete|merge|with)[ \\t\\(\\[].*");
    private static final Set<String> SYSTEM_BUILTIN_FUNCTIONS = ImmutableSet.of(
            "analyze_logical_plan",
            "exclude_columns",
            "sequence");

    private final GalaxyAccessControllerSupplier controllerSupplier;
    private final int backgroundProcessingThreads;
    private final ExecutorService backgroundExecutorService;

    public GalaxyAccessControl(int backgroundProcessingThreads, GalaxyAccessControllerSupplier controllerSupplier)
    {
        this.backgroundProcessingThreads = backgroundProcessingThreads;
        this.backgroundExecutorService = new ThreadPoolExecutor(
                backgroundProcessingThreads,
                backgroundProcessingThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(MAX_OUTSTANDING_BACKGROUND_TASKS),
                daemonThreadsNamed("galaxy-access-control-%s"));
        this.controllerSupplier = requireNonNull(controllerSupplier, "controllerSupplier is null");
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support user impersonation");
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        // Allow - this is a deprecated check that must be allowed, or all queries will fail
    }

    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
        // Allow - cluster access is enforced in the dispatcher
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        checkRoleOwnsQuery(identity, queryOwner, AccessDeniedException::denyViewQuery);
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        return filterIdentitiesByPrivilegesAndRoles(identity, queryOwners);
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        checkRoleOwnsQuery(identity, queryOwner, AccessDeniedException::denyKillQuery);
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support directly reading Trino cluster system information");
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support directly writing Trino cluster system information");
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        // Allow - all session properties are currently allowed
    }

    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        return true;
        // Allow - the granular check in this class are called after this check
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalog)
    {
        // Galaxy manages catalogs for users so we do not intend to support CREATE CATALOG.
        denyCreateCatalog(catalog);
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalog)
    {
        // Galaxy manages catalogs for users so we do not intend to support DROP CATALOG.
        denyDropCatalog(catalog);
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        GalaxySystemAccessController controller = getSystemAccessController(context);

        Set<String> needFiltering = catalogs.stream()
                .filter(catalog -> !isSystemCatalog(catalog) && !controller.hasImpliedCatalogVisibility(context, catalog))
                .collect(toImmutableSet());
        if (needFiltering.isEmpty()) {
            return catalogs;
        }
        Predicate<String> catalogVisibility = controller.getCatalogVisibility(context, needFiltering);
        return catalogs.stream()
                .filter(catalogName -> !needFiltering.contains(catalogName) || catalogVisibility.test(catalogName))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        checkCatalogIsWritable(getSystemAccessController(context), schema.getCatalogName(), explanation -> denyCreateSchema(schema.toString(), explanation));
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

        Set<String> requestedSchemaNames = schemaNames.stream()
                .filter(schemaName -> !isInformationSchema(schemaName))
                .collect(toImmutableSet());

        if (requestedSchemaNames.isEmpty()) {
            return schemaNames;
        }

        Predicate<String> schemaVisibility = getVisibilityForSchemas(context, catalogName, requestedSchemaNames);

        return schemaNames.stream()
                .filter(name -> isInformationSchema(name) || schemaVisibility.test(name))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        if (!isSystemOrInformationSchema(schemaName) &&
                !getVisibilityForSchemas(context, schemaName.getCatalogName(), ImmutableSet.of(schemaName.getSchemaName())).test(schemaName.getSchemaName())) {
            denyShowCreateSchema(schemaName.toString(), entityIsNotVisible(context, "Schema", schemaName.toString()));
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!isSystemOrInformationSchema(table) && !isTableVisible(context, table)) {
            denyShowCreateTable(table.toString(), entityIsNotVisible(context, "Table", table.toString()));
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        checkCatalogIsWritable(getSystemAccessController(context), table.getCatalogName(), explanation -> denyCreateTable(table.toString(), explanation));
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
        Predicate<SchemaTableName> tableVisibility = getTableVisibility(context, catalogName, tableNames);
        return tableNames.stream()
                .filter(name -> isInformationSchema(name.getSchemaName()) || tableVisibility.test(name))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (isSystemCatalog(table.getCatalogName())) {
            // TODO what about information_schema? filterColumns() is no-op for information_schema. See also https://github.com/starburstdata/stargate/issues/12879
            return;
        }
        GalaxySystemAccessController controller = getSystemAccessController(context);
        EntityPrivileges privileges = controller.getEntityPrivileges(context, toTableId(controller, table).orElseThrow(() -> new TableNotFoundException(table.getSchemaTableName())));

        // Check that the user has wildcard SELECT column privilege
        ContentsVisibility visibility = privileges.getColumnPrivileges().get(SELECT.name());
        if (visibility != null && visibility.defaultVisibility() == GrantKind.ALLOW && visibility.overrideVisibility().isEmpty()) {
            controller.implyCatalogVisibility(context, table.getCatalogName());
            return;
        }
        denyShowColumns(table.toString(), roleLacksPrivilege(context, SELECT, "all columns of", table.toString()));
    }

    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (isSystemOrInformationSchema(table)) {
            return columns;
        }
        GalaxySystemAccessController controller = getSystemAccessController(context);
        EntityPrivileges privileges = controller.getEntityPrivileges(context, toTableId(controller, table).orElseThrow(() -> new TableNotFoundException(table.getSchemaTableName())));
        ContentsVisibility visibility = privileges.getColumnPrivileges().get(SELECT.name());
        if (visibility == null) {
            visibility = privileges.getColumnPrivileges().get("VISIBLE");
            if (visibility == null) {
                return ImmutableSet.of();
            }
        }

        Set<String> filtered = columns.stream()
                .filter(visibility::isVisible)
                .collect(toImmutableSet());
        if (!filtered.isEmpty()) {
            controller.implyCatalogVisibility(context, table.getCatalogName());
        }
        return filtered;
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SystemSecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        FilterColumnsAcceleration mode = getFilterColumnsAcceleration(((FullSystemSecurityContext) context).getSession());
        return switch (mode) {
            case NONE -> SystemAccessControl.super.filterColumns(context, catalogName, tableColumns);
            case FCX2 -> {
                Map<SchemaTableName, Set<String>> results = new ConcurrentHashMap<>();
                // Use main thread to ensure the use of background processing threads increases parallelism.
                // Otherwise, for larger number of callers of GalaxyAccessControl, the shared pool could be a bottleneck.
                processWithAdditionalThreads(
                        backgroundExecutorService,
                        backgroundProcessingThreads,
                        tableColumns.entrySet(),
                        entry -> {
                            results.put(entry.getKey(), filterColumns(context, new CatalogSchemaTableName(catalogName, entry.getKey()), entry.getValue()));
                        });

                yield results;
            }
        };
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
            checkHasCatalogPrivilege(context, table.getCatalogName(), CREATE_SCHEMA, explanation -> denySelectColumns(table.toString(), columns, explanation));
            return;
        }
        if (isSystemOrInformationSchema(table)) {
            return;
        }
        checkHasPrivilegeOnColumns(context, SELECT, false, table, columns, explanation -> denySelectColumns(table.toString(), columns, explanation));
        if (!columns.isEmpty()) {
            getSystemAccessController(context).implyCatalogVisibility(context, table.getCatalogName());
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
        checkCatalogIsWritable(getSystemAccessController(context), table.getCatalogName(), explanation -> denyUpdateTableColumns(table.toString(), columns, explanation));
        checkHasPrivilegeOnColumns(context, UPDATE, false, table, columns, explanation -> denyUpdateTableColumns(table.toString(), columns, explanation));
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        checkCatalogIsWritable(getSystemAccessController(context), view.getCatalogName(), explanation -> denyCreateView(view.toString(), explanation));
        checkHasSchemaPrivilege(context, view, CREATE_TABLE, explanation -> denyCreateView(view.toString(), explanation));
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        Consumer<String> denier = explanation -> denyRenameView(view.toString(), newView.toString(), explanation);
        checkCatalogWritableAndViewOwner(context, view, denier);
        checkCatalogIsWritable(getSystemAccessController(context), newView.getCatalogName(), denier);
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
        checkHasPrivilegeOnColumns(context, SELECT, !isViewOwnerQuerying(context), table, columns, explanation ->
                denyCreateViewWithSelect(table.toString(), context.getIdentity().toConnectorIdentity(), explanation));
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        checkCatalogIsWritable(getSystemAccessController(context), materializedView.getCatalogName(), explanation -> denyCreateMaterializedView(materializedView.toString(), explanation));
        checkHasSchemaPrivilege(context, materializedView, CREATE_TABLE, explanation -> denyCreateMaterializedView(materializedView.toString(), explanation));
        checkCreateMaterializedViewProperties(context, materializedView, properties);
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
        checkCatalogIsWritable(getSystemAccessController(context), newView.getCatalogName(), explanation -> denyRenameMaterializedView(materializedView.toString(), newView.toString(), explanation));
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        // Allow - all session properties are currently allowed
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        validateEntityKindPrivilege("granted", EntityKind.SCHEMA, privilege);
        // Galaxy does the checking
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        validateEntityKindPrivilege("denied", EntityKind.SCHEMA, privilege);
        // Galaxy does the checking
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        validateEntityKindPrivilege("revoked", EntityKind.SCHEMA, privilege);
        // Galaxy does the checking
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption)
    {
        validateEntityKindPrivilege("granted", EntityKind.TABLE, privilege);
        // Galaxy does the checking
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        validateEntityKindPrivilege("denied", EntityKind.TABLE, privilege);
        // Galaxy does the checking
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, io.trino.spi.security.Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption)
    {
        validateEntityKindPrivilege("revoked", EntityKind.TABLE, privilege);
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
    public boolean canExecuteFunction(SystemSecurityContext context, CatalogSchemaRoutineName function)
    {
        if (isWhitelistedTableFunction(function)) {
            return true;
        }
        GalaxySystemAccessController controller = getSystemAccessController(context);

        Optional<CatalogId> catalogId = controller.getCatalogId(function.getCatalogName());
        if (catalogId.isEmpty()) {
            return false;
        }
        FunctionId functionId = new FunctionId(catalogId.get(), function.getSchemaName(), function.getRoutineName());

        if (isInGalaxyFunctionsSchema(function)) {
            return controller.canExecuteUserDefinedFunction(context, functionId);
        }

        // Otherwise it might be a table function
        return controller.canExecuteFunction(context, functionId);
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName functionName)
    {
        // TODO (https://github.com/starburstdata/stargate/issues/12361) implement. This is needed for views containing table functions.
        // Not allowed unless the identity user is a view definer user
        return systemSecurityContext.getIdentity().getUser().startsWith("<galaxy role ");
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext context, CatalogSchemaTableName table, String procedure)
    {
        if (isSystemCatalog(table.getCatalogName())) {
            denyExecuteProcedure(procedure, "Table procedures are not permitted on the system catalog");
        }
        checkIsTableOwner(context, table, explanation -> denyExecuteProcedure(procedure, explanation));
    }

    @Override
    public void checkCanShowFunctions(SystemSecurityContext context, CatalogSchemaName schema)
    {}

    @Override
    public Set<SchemaFunctionName> filterFunctions(SystemSecurityContext context, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        return functionNames;
    }

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        if (isSystemOrInformationSchema(tableName)) {
            return ImmutableList.of();
        }
        GalaxySystemAccessController controller = getSystemAccessController(context);
        Optional<TableId> tableId = toTableId(controller, tableName);
        if (tableId.isEmpty()) {
            return ImmutableList.of();
        }
        return controller.getRowFilters(context, tableId.get());
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type)
    {
        if (isSystemOrInformationSchema(tableName)) {
            return Optional.empty();
        }
        GalaxySystemAccessController controller = getSystemAccessController(context);
        Optional<TableId> tableId = toTableId(controller, tableName);
        if (tableId.isEmpty()) {
            return Optional.empty();
        }
        return controller.getColumnMask(context, columnName, tableId.get());
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return ImmutableSet.of();
    }

    @Override
    public void checkCanCreateFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        if (!isInGalaxyFunctionsSchema(functionName) || !hasAccountPrivilege(context.getIdentity(), CREATE_FUNCTION)) {
            denyCreateFunction(functionName.getRoutineName());
        }
    }

    @Override
    public void checkCanDropFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
    {
        if (!isInGalaxyFunctionsSchema(functionName)) {
            denyDropFunction(functionName.getRoutineName());
        }
        if (hasAccountPrivilege(context.getIdentity(), CREATE_FUNCTION)) {
            return;
        }
        CatalogId catalogId = getSystemAccessController(context.getIdentity()).getCatalogId(functionName.getCatalogName())
                .orElseThrow(NotFoundException::new);
        FunctionId functionId = new FunctionId(catalogId, functionName.getSchemaName(), functionName.getRoutineName());
        if (!isEntityOwner(controllerSupplier.apply(context.getIdentity()), context, Optional.of(functionId))) {
            denyDropFunction(functionName.getRoutineName());
        }
    }

    // Helper methods that provide explanations for denied access

    private void checkRoleOwnsQuery(Identity identity, Identity queryOwner, Consumer<String> denier)
    {
        if (!getSystemAccessController(identity).listEnabledRoles(identity).containsValue(getRoleId(queryOwner))) {
            denier.accept(roleIsNotQueryOwner(identity, queryOwner));
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
        if (!hasEntityPrivilege(context, getSystemAccessController(context).getCatalogId(catalogName), privilege, false)) {
            denier.accept(roleLacksPrivilege(context, privilege, "catalog", catalogName));
        }
    }

    private void checkHasSchemaPrivilege(SystemSecurityContext context, CatalogSchemaTableName table, Privilege privilege, Consumer<String> denier)
    {
        if (!hasEntityPrivilege(context, toSchemaId(getSystemAccessController(context), table), privilege, false)) {
            denier.accept(roleLacksPrivilege(context, privilege, "schema", new CatalogSchemaName(table.getCatalogName(), table.getSchemaTableName().getSchemaName()).toString()));
        }
    }

    private void checkHasTablePrivilege(SystemSecurityContext context, CatalogSchemaTableName table, Privilege privilege, Consumer<String> denier)
    {
        Optional<TableId> tableId = toTableId(getSystemAccessController(context), table);
        if (!hasEntityPrivilege(context, tableId, privilege, false)) {
            denier.accept(roleLacksPrivilege(context, privilege, "table", table.toString()));
        }
    }

    private void checkHasPrivilegeOnColumns(SystemSecurityContext context, Privilege privilege, boolean requiresGrantOption, CatalogSchemaTableName table, Set<String> columns, Consumer<String> denier)
    {
        GalaxySystemAccessController controller = getSystemAccessController(context);
        Optional<TableId> tableId = toTableId(controller, table);
        if (tableId.isEmpty()) {
            runPrivilegeDenier(context, privilege, requiresGrantOption, columns, denier);
        }
        EntityPrivileges privileges = controller.getEntityPrivileges(context, tableId.get());
        if (requiresGrantOption) {
            Optional<GalaxyPrivilegeInfo> info = privileges.getPrivileges().stream().filter(element -> element.getPrivilege() == privilege && element.isGrantOption()).findAny();
            if (info.isEmpty() || !info.get().isGrantOption()) {
                runPrivilegeDenier(context, privilege, requiresGrantOption, columns, denier);
            }
        }
        ContentsVisibility visibility = privileges.getColumnPrivileges().get(privilege.name());
        Set<String> deniedColumns;
        // If there is no column visibility, or if default column visibility is DENY and there are no overrides, all columns are denied
        if (visibility == null || (visibility.defaultVisibility() == GrantKind.DENY && visibility.overrideVisibility().isEmpty())) {
            denier.accept("Relation not found or not allowed");
        }
        else {
            deniedColumns = columns.stream().filter(column -> !visibility.isVisible(column)).collect(toImmutableSet());
            if (!deniedColumns.isEmpty()) {
                runPrivilegeDenier(context, privilege, requiresGrantOption, deniedColumns, denier);
            }
        }
    }

    private void runPrivilegeDenier(SystemSecurityContext context, Privilege privilege, boolean requiresGrantOption, Set<String> deniedColumns, Consumer<String> denier)
    {
        String kind = deniedColumns.size() == 1 ? "column" : "columns";
        denier.accept(roleLacksPrivilege(context, privilege, requiresGrantOption, kind, deniedColumns.isEmpty() ? "" : "[%s]".formatted(String.join(", ", deniedColumns))));
    }

    private boolean hasAccountPrivilege(Identity identity, Privilege privilege)
    {
        GalaxySystemAccessController controller = getSystemAccessController(identity);
        AccountId accountId = controller.getAccountId(identity);
        EntityPrivileges entityPrivileges = controller.getEntityPrivileges(identity, accountId);
        return hasEntityPrivilege(accountId, entityPrivileges, privilege, false);
    }

    private void checkCatalogWritableAndSchemaOwner(SystemSecurityContext context, CatalogSchemaName schema, Consumer<String> denier)
    {
        checkCatalogIsWritable(getSystemAccessController(context), schema.getCatalogName(), denier);
        checkIsSchemaOwner(context, schema, denier);
    }

    private void checkCatalogWritableAndTableOwner(SystemSecurityContext context, CatalogSchemaTableName table, Consumer<String> denier)
    {
        checkCatalogIsWritable(getSystemAccessController(context), table.getCatalogName(), denier);
        checkIsTableOwner(context, table, denier);
    }

    public void checkCatalogWritableAndViewOwner(SystemSecurityContext context, CatalogSchemaTableName view, Consumer<String> denier)
    {
        checkCatalogIsWritable(getSystemAccessController(context), view.getCatalogName(), denier);
        checkIsViewOwner(context, view, denier);
    }

    public void checkCatalogWritableAndMaterializedViewOwner(SystemSecurityContext context, CatalogSchemaTableName view, Consumer<String> denier)
    {
        checkCatalogIsWritable(getSystemAccessController(context), view.getCatalogName(), denier);
        if (!isTableOwner(context, view)) {
            denier.accept(format("Role %s does not own the materialized view", currentRoleName(context)));
        }
    }

    public static boolean canSkipListEnabledRoles(String query)
    {
        String trimmedQuery = query.trim();
        String candidateString = trimmedQuery.substring(0, Math.min(10, query.length())).toLowerCase(Locale.ENGLISH);
        return DML_OPERATION_MATCHER.matcher(candidateString).matches();
    }

    private void checkCatalogWritableAndHasTablePrivilege(SystemSecurityContext context, CatalogSchemaTableName table, Privilege privilege, Consumer<String> denier)
    {
        checkCatalogIsWritable(getSystemAccessController(context), table.getCatalogName(), denier);
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

    /**
     * Make sure the current active role set has CREATE_TABLE on the schema specified by the
     * storage_schema materialized view property, if it exists, to prevent creating a table in a schema
     * the active role set doesn't have privileges to do so.
     */
    private void checkCreateMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        if (properties != null) {
            Optional<String> storageSchema = Optional.ofNullable(properties.get("storage_schema")).map(String::valueOf);
            if (storageSchema.isPresent() && !storageSchema.get().equals(materializedView.getSchemaTableName().getSchemaName())) {
                CatalogSchemaTableName storageSchemaTable = new CatalogSchemaTableName(materializedView.getCatalogName(), new SchemaTableName(storageSchema.get(), materializedView.getSchemaTableName().getTableName()));
                checkHasSchemaPrivilege(context, storageSchemaTable, CREATE_TABLE, explanation -> denyCreateMaterializedView(storageSchemaTable.toString(), explanation));
            }
        }
    }

    private String entityIsNotVisible(SystemSecurityContext context, String description, String entityName)
    {
        return format("%s %s is not visible to the role %s", entityName, description, currentRoleName(context));
    }

    private String roleIsNotQueryOwner(Identity identity, Identity queryOwner)
    {
        return format(
                "Role %s does not own the query",
                getSystemAccessController(identity).getRoleDisplayName(identity, getRoleId(queryOwner)));
    }

    private String catalogIsReadOnly(String catalog)
    {
        return format("Catalog %s only allows read-only access", catalog);
    }

    private String roleLacksPrivilege(SystemSecurityContext context, Privilege privilege, String kind, String entity)
    {
        return roleLacksPrivilege(context, privilege, false, kind, entity);
    }

    private String roleLacksPrivilege(SystemSecurityContext context, Privilege privilege, boolean requiresGrantOption, String kind, String entity)
    {
        return format("Role %s does not have the privilege %s%s on the %s %s", currentRoleName(context), privilege, requiresGrantOption ? " WITH GRANT OPTION" : "", kind, entity);
    }

    // Helper methods that call Galaxy to determine access

    private boolean hasEntityPrivilege(SystemSecurityContext context, Optional<? extends EntityId> entity, Privilege privilege, boolean requiresGrantOption)
    {
        if (entity.isEmpty()) {
            return false;
        }
        EntityId entityId = entity.get();
        EntityPrivileges entityPrivileges = getSystemAccessController(context).getEntityPrivileges(context, entityId);
        return hasEntityPrivilege(entityId, entityPrivileges, privilege, requiresGrantOption);
    }

    private boolean hasEntityPrivilege(EntityId entityId, EntityPrivileges entityPrivileges, Privilege privilege, boolean requiresGrantOption)
    {
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
        return getVisibilityForSchemas(context, schema.getCatalogName(), ImmutableSet.of(schema.getSchemaName())).test(schema.getSchemaName());
    }

    private Predicate<String> getVisibilityForSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        Optional<CatalogId> catalogId = getSystemAccessController(context).getCatalogId(catalogName);
        if (catalogId.isEmpty()) {
            return ignored -> false;
        }
        return getSystemAccessController(context).getVisibilityForSchemas(context, catalogId.get(), schemaNames);
    }

    private List<Identity> filterIdentitiesByPrivilegesAndRoles(Identity identity, Collection<Identity> identities)
    {
        // VIEW_ALL_QUERY_HISTORY is a special galaxy privilege granting access to all queries
        if (hasAccountPrivilege(identity, VIEW_ALL_QUERY_HISTORY)) {
            return ImmutableList.copyOf(identities);
        }
        return filterIdentitiesByActiveRoleSet(identity, identities);
    }

    private List<Identity> filterIdentitiesByActiveRoleSet(Identity identity, Collection<Identity> identities)
    {
        Set<RoleId> enabledRoles = ImmutableSet.copyOf(getSystemAccessController(identity).listEnabledRoles(identity).values());
        return identities.stream()
                .filter(otherIdentity -> enabledRoles.contains(getRoleId(otherIdentity)))
                .collect(toImmutableList());
    }

    private boolean isSchemaOwner(SystemSecurityContext context, CatalogSchemaName schema)
    {
        GalaxySystemAccessController controller = getSystemAccessController(context);
        return isEntityOwner(controller, context, toSchemaId(controller, schema));
    }

    private boolean isTableOwner(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        GalaxySystemAccessController controller = getSystemAccessController(context);
        return isEntityOwner(controller, context, toTableId(controller, tableName));
    }

    private boolean isTableVisible(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        return getTableVisibility(context, tableName.getCatalogName(), ImmutableSet.of(tableName.getSchemaTableName()))
                .test(tableName.getSchemaTableName());
    }

    private Predicate<SchemaTableName> getTableVisibility(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        if (isSystemCatalog(catalogName)) {
            return ignored -> true;
        }
        Optional<CatalogId> optionalCatalogId = getSystemAccessController(context).getCatalogId(catalogName);
        if (optionalCatalogId.isEmpty()) {
            return ignored -> false;
        }
        CatalogId catalogId = optionalCatalogId.get();

        Set<String> schemaNames = tableNames.stream()
                .map(SchemaTableName::getSchemaName)
                .filter(schema -> !isInformationSchema(schema))
                .collect(toImmutableSet());

        // getTableVisibility is no-op when schemaNames.isEmpty, i.e. only information_schema schema
        if (schemaNames.isEmpty()) {
            return ignored -> true;
        }

        // If there is only one schemaName and ten or fewer table names,
        // use the (presumably) faster method getVisbilityForTables.    We think
        // that if there are more tables, it might be faster to call
        // getTableVisiblity.
        if (tableNames.size() <= 10 && schemaNames.size() == 1) {
            GalaxySystemAccessController controller = controllerSupplier.apply(context.getIdentity());
            Set<String> tableNameStrings = tableNames.stream().map(SchemaTableName::getTableName).collect(toImmutableSet());
            ContentsVisibility visibility = controllerSupplier.apply(context.getIdentity())
                    .getVisibilityForTables(context, catalogId, Iterables.getOnlyElement(schemaNames), tableNameStrings);
            return tableName -> isInformationSchema(tableName.getSchemaName()) || visibility.isVisible(tableName.getTableName());
        }

        // Otherwise find visibility for all the schemaNames
        return getSystemAccessController(context).getTableVisibility(context, catalogId, schemaNames);
    }

    private boolean isEntityOwner(GalaxySystemAccessController controller, SystemSecurityContext context, Optional<? extends EntityId> entity)
    {
        if (entity.isEmpty()) {
            return false;
        }

        EntityPrivileges entityPrivileges = controller.getEntityPrivileges(context, entity.get());
        return entityPrivileges.isOwnerInActiveRoleSet();
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

    private boolean isViewOwnerQuerying(SystemSecurityContext context)
    {
        // When querying a view, the view's run-as role will be different from the actual role executing the query
        RoleId viewOwner = getContextRoleId(context.getIdentity());
        return getSystemAccessController(context)
                .listEnabledRoles(context.getIdentity(), GalaxyIdentity::toDispatchSessionFromPrincipal)
                .containsValue(viewOwner);
    }

    private boolean isReadOnlyCatalog(GalaxySystemAccessController controller, String name)
    {
        return isSystemCatalog(name) || controller.isReadOnlyCatalog(name);
    }

    private String currentRoleName(SystemSecurityContext context)
    {
        return getSystemAccessController(context).getRoleDisplayName(context.getIdentity(), getRoleId(context.getIdentity()));
    }

    private GalaxySystemAccessController getSystemAccessController(SystemSecurityContext context)
    {
        return getSystemAccessController(context.getIdentity());
    }

    private GalaxySystemAccessController getSystemAccessController(Identity identity)
    {
        return controllerSupplier.apply(identity);
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
        return table.getSchemaTableName().equals(new SchemaTableName("schema_discovery", "discovery")) ||
                table.getSchemaTableName().equals(new SchemaTableName("schema_discovery", "shallow_discovery"));
    }

    private static boolean isSystemOrInformationSchema(CatalogSchemaName name)
    {
        return isSystemCatalog(name.getCatalogName()) || isInformationSchema(name.getSchemaName());
    }

    private static boolean isSystemOrInformationSchema(CatalogSchemaTableName name)
    {
        CatalogSchemaName catalogSchemaName = new CatalogSchemaName(name.getCatalogName(), name.getSchemaTableName().getSchemaName());
        return isSystemOrInformationSchema(catalogSchemaName);
    }

    private static void validateEntityKindPrivilege(String operation, EntityKind entityKind, io.trino.spi.security.Privilege privilege)
    {
        if (!getPrivilegesForEntityKind(entityKind).contains(privilege)) {
            throw operationNotAllowed("Privilege %s may not be %s to entity kind %s".formatted(privilege, operation, entityKind));
        }
    }

    private static boolean isWhitelistedTableFunction(CatalogSchemaRoutineName catalogSchemaRoutineName)
    {
        return isSystemCatalog(catalogSchemaRoutineName.getCatalogName()) &&
                BUILTIN_SCHEMA.equals(catalogSchemaRoutineName.getSchemaName()) &&
                SYSTEM_BUILTIN_FUNCTIONS.contains(catalogSchemaRoutineName.getRoutineName());
    }

    public static boolean isInGalaxyFunctionsSchema(CatalogSchemaRoutineName routineName)
    {
        return routineName.getCatalogName().equals("galaxy") && routineName.getSchemaName().equals("functions");
    }
}
