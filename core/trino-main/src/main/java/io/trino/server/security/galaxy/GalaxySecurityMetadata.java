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
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.client.CreateEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.CreateRoleGrant;
import io.starburst.stargate.accesscontrol.client.GalaxyPrincipal;
import io.starburst.stargate.accesscontrol.client.RevokeEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.TableGrant;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.ColumnId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.SchemaId;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.PrivilegeInfo;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.ALLOW;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.DENY;
import static io.trino.server.security.galaxy.GalaxyIdentity.createViewOwnerIdentity;
import static io.trino.server.security.galaxy.GalaxyIdentity.toDispatchSession;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.CREATE;
import static io.trino.spi.security.Privilege.DELETE;
import static io.trino.spi.security.Privilege.INSERT;
import static io.trino.spi.security.Privilege.SELECT;
import static io.trino.spi.security.Privilege.UPDATE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class GalaxySecurityMetadata
        implements SystemSecurityMetadata
{
    public static final BiMap<Privilege, io.starburst.stargate.accesscontrol.privilege.Privilege> PRIVILEGE_TRANSLATIONS = ImmutableBiMap.<Privilege, io.starburst.stargate.accesscontrol.privilege.Privilege>builder()
            .put(SELECT, io.starburst.stargate.accesscontrol.privilege.Privilege.SELECT)
            .put(INSERT, io.starburst.stargate.accesscontrol.privilege.Privilege.INSERT)
            .put(DELETE, io.starburst.stargate.accesscontrol.privilege.Privilege.DELETE)
            .put(UPDATE, io.starburst.stargate.accesscontrol.privilege.Privilege.UPDATE)
            .put(CREATE, io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_TABLE)
            .build();
    private static final TrinoPrincipal SYSTEM_ROLE = new TrinoPrincipal(ROLE, "_system");
    private static final TrinoPrincipal PUBLIC_ROLE = new TrinoPrincipal(ROLE, "public");

    private final TrinoSecurityApi accessControlClient;
    private final CatalogIds catalogIds;

    @Inject
    public GalaxySecurityMetadata(TrinoSecurityApi accessControlClient, CatalogIds catalogIds)
    {
        this.accessControlClient = requireNonNull(accessControlClient, "accessControlClient is null");
        this.catalogIds = requireNonNull(catalogIds, "catalogIds is null");
    }

    @Override
    public boolean roleExists(Session session, String role)
    {
        return accessControlClient.roleExists(toDispatchSession(session), new RoleName(role));
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor)
    {
        if (grantor.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support creating a role with an explicit grantor");
        }
        accessControlClient.createRole(toDispatchSession(session), new RoleName(role));
    }

    @Override
    public void dropRole(Session session, String role)
    {
        accessControlClient.dropRole(toDispatchSession(session), new RoleName(role));
    }

    @Override
    public Set<String> listRoles(Session session)
    {
        return accessControlClient.listRoles(toDispatchSession(session)).keySet().stream()
                .map(RoleName::getName)
                .collect(toImmutableSet());
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, TrinoPrincipal principal)
    {
        return toTrinoRoleGrants(accessControlClient.listRoleGrants(toDispatchSession(session), toGalaxyPrincipal(principal), false));
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        if (grantor.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support GRANT with the GRANTED BY clause");
        }

        DispatchSession dispatchSession = toDispatchSession(session);
        ImmutableSet.Builder<CreateRoleGrant> roleGrants = ImmutableSet.builder();
        for (String role : roles) {
            for (TrinoPrincipal grantee : grantees) {
                if (adminOption && grantee.getType() == USER) {
                    throw new TrinoException(NOT_SUPPORTED, "Galaxy only supports a ROLE for GRANT with ADMIN OPTION");
                }
                roleGrants.add(new CreateRoleGrant(new RoleName(role), toGalaxyPrincipal(grantee), adminOption));
            }
        }

        accessControlClient.grantRoles(dispatchSession, roleGrants.build());
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        if (grantor.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy does not support REVOKE with the GRANTED BY clause");
        }
        ImmutableSet.Builder<io.starburst.stargate.accesscontrol.client.RoleGrant> roleGrants = ImmutableSet.builder();
        for (String role : roles) {
            for (TrinoPrincipal grantee : grantees) {
                roleGrants.add(new io.starburst.stargate.accesscontrol.client.RoleGrant(toGalaxyPrincipal(grantee), new RoleName(role), adminOption));
            }
        }
        accessControlClient.revokeRoles(toDispatchSession(session), roleGrants.build());
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal)
    {
        return toTrinoRoleGrants(accessControlClient.listRoleGrants(toDispatchSession(session), toGalaxyPrincipal(principal), true));
    }

    @Override
    public Set<String> listEnabledRoles(Identity identity)
    {
        return accessControlClient.listEnabledRoles(toDispatchSession(identity)).keySet().stream()
                .map(RoleName::getName)
                .collect(toImmutableSet());
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        addEntityPrivilege(session, toSchemaEntity(schemaName), privileges, ALLOW, grantee, grantOption);
    }

    @Override
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        addEntityPrivilege(session, toSchemaEntity(schemaName), privileges, DENY, grantee, false);
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        revokeEntityPrivileges(session, toSchemaEntity(schemaName), privileges, grantee, grantOption);
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        addEntityPrivilege(session, toTableEntity(tableName), privileges, ALLOW, grantee, grantOption);
    }

    @Override
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        addEntityPrivilege(session, toTableEntity(tableName), privileges, DENY, grantee, false);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        revokeEntityPrivileges(session, toTableEntity(tableName), privileges, grantee, grantOption);
    }

    // TODO: Are these methods candidates for inclusion in SystemSecurityMetatadata?

    public void grantColumnPrivileges(Session session, QualifiedObjectName tableName, String columnName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        addEntityPrivilege(session, toColumnEntity(tableName, columnName), privileges, ALLOW, grantee, grantOption);
    }

    public void denyColumnPrivileges(Session session, QualifiedObjectName tableName, String columnName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        addEntityPrivilege(session, toColumnEntity(tableName, columnName), privileges, DENY, grantee, false);
    }

    public void revokeColumnPrivileges(Session session, QualifiedObjectName tableName, String columnName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        revokeEntityPrivileges(session, toColumnEntity(tableName, columnName), privileges, grantee, grantOption);
    }

    private void addEntityPrivilege(Session session, EntityId entityId, Set<Privilege> privileges, GrantKind allow, TrinoPrincipal grantee, boolean grantOption)
    {
        if (grantee.getType() != ROLE) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy only supports a ROLE as a grantee");
        }
        RoleName granteeRole = new RoleName(grantee.getName());

        Set<CreateEntityPrivilege> entityPrivileges = privileges.stream()
                .map(privilege -> new CreateEntityPrivilege(toGalaxyPrivilege(privilege), allow, granteeRole, grantOption))
                .collect(toImmutableSet());
        accessControlClient.addEntityPrivileges(toDispatchSession(session), entityId, entityPrivileges);
    }

    private void revokeEntityPrivileges(Session session, EntityId entityId, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        if (grantee.getType() != ROLE) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy only supports a ROLE as a grantee");
        }
        RoleName granteeRole = new RoleName(grantee.getName());

        Set<RevokeEntityPrivilege> entityPrivileges = privileges.stream()
                .map(privilege -> new RevokeEntityPrivilege(toGalaxyPrivilege(privilege), granteeRole, grantOption))
                .collect(toImmutableSet());
        accessControlClient.revokeEntityPrivileges(toDispatchSession(session), entityId, entityPrivileges);
    }

    @Override
    public Set<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        if (isSystemCatalog(prefix.getCatalogName())) {
            return ImmutableSet.of(new GrantInfo(
                    new PrivilegeInfo(SELECT, false),
                    PUBLIC_ROLE,
                    schemaTableName("*", "*"),
                    Optional.empty(),
                    Optional.empty()));
        }
        CatalogId catalogId = translateCatalogNameToId(prefix.getCatalogName());
        EntityId entityId;
        if (prefix.getTableName().isPresent()) {
            entityId = new TableId(catalogId, prefix.getSchemaName().orElseThrow(), prefix.getTableName().get());
        }
        else if (prefix.getSchemaName().isPresent()) {
            entityId = new SchemaId(catalogId, prefix.getSchemaName().get());
        }
        else {
            entityId = catalogId;
        }

        return accessControlClient.listTableGrants(toDispatchSession(session), entityId).stream()
                // Trino's GrantInfo does not contain grantKind.  For now, only show ALLOW privileges,
                // pending a change to Trino to pass GrantKind through
                .filter(details -> details.getGrantKind() == ALLOW)
                .filter(details -> PRIVILEGE_TRANSLATIONS.inverse().containsKey(details.getPrivilege()))
                .map(details -> toGrantInfo(details, new SchemaTableName(details.getTableId().getSchemaName(), details.getTableId().getTableName())))
                .collect(toImmutableSet());
    }

    private static GrantInfo toGrantInfo(TableGrant tableGrant, SchemaTableName tableName)
    {
        Privilege privilege = PRIVILEGE_TRANSLATIONS.inverse().get(tableGrant.getPrivilege());
        checkArgument(privilege != null, "Could not find Trino privilege for Galaxy privilege %s, in PrivilegeDetails %s", tableGrant.getPrivilege(), tableGrant);
        return new GrantInfo(
                new PrivilegeInfo(privilege, tableGrant.isGrantOption()),
                new TrinoPrincipal(ROLE, tableGrant.getGrantee().getName()),
                tableName,
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schema)
    {
        if (isSystemCatalog(schema.getCatalogName())) {
            return Optional.of(SYSTEM_ROLE);
        }
        RoleName owner = accessControlClient.getEntityPrivileges(toDispatchSession(session), toSchemaEntity(schema)).getOwner();
        return Optional.of(new TrinoPrincipal(ROLE, owner.getName()));
    }

    @Override
    public void setSchemaOwner(Session session, CatalogSchemaName schema, TrinoPrincipal owner)
    {
        throwIfSystemCatalog(schema);
        if (owner.getType() != ROLE) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy only supports a ROLE as an owner");
        }

        accessControlClient.setEntityOwner(
                toDispatchSession(session),
                toSchemaEntity(schema),
                new RoleName(owner.getName()));
    }

    @Override
    public void setTableOwner(Session session, CatalogSchemaTableName table, TrinoPrincipal owner)
    {
        throwIfSystemCatalog(table);
        if (owner.getType() != ROLE) {
            throw new TrinoException(NOT_SUPPORTED, "Galaxy only supports a ROLE as an owner");
        }

        accessControlClient.setEntityOwner(
                toDispatchSession(session),
                toTableEntity(new QualifiedObjectName(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName())),
                new RoleName(owner.getName()));
    }

    /**
     * Create an identity with the synthetic user name, the owner role in the credentials, and
     * enabled roles set consisting only of the owner role.
     */
    @Override
    public Optional<Identity> getViewRunAsIdentity(Session session, CatalogSchemaTableName viewName)
    {
        DispatchSession dispatchSession = toDispatchSession(session);
        EntityPrivileges privileges = accessControlClient.getEntityPrivileges(dispatchSession, dispatchSession.getRoleId(), toTableEntity(viewName));
        if (!privileges.isExplicitOwner()) {
            throw new TrinoException(INVALID_VIEW, format("View '%s' is disabled until an explicit owner role is set", viewName));
        }
        return Optional.of(createViewOwnerIdentity(session.getIdentity(), privileges.getOwner(), privileges.getOwnerId()));
    }

    @Override
    public void setViewOwner(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        setTableOwner(session, view, principal);
    }

    @Override
    public void schemaCreated(Session session, CatalogSchemaName schema)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(schema);
        accessControlClient.entityCreated(toDispatchSession(session), toSchemaEntity(schema));
    }

    @Override
    public void schemaRenamed(Session session, CatalogSchemaName sourceSchema, CatalogSchemaName targetSchema)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(sourceSchema);
        throwIfSystemCatalog(targetSchema);
        accessControlClient.entityRenamed(toDispatchSession(session), toSchemaEntity(sourceSchema), toSchemaEntity(targetSchema));
    }

    @Override
    public void schemaDropped(Session session, CatalogSchemaName schema)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(schema);
        accessControlClient.entityDropped(toDispatchSession(session), toSchemaEntity(schema));
    }

    @Override
    public void tableCreated(Session session, CatalogSchemaTableName table)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        accessControlClient.entityCreated(toDispatchSession(session), toTableEntity(table));
    }

    @Override
    public void tableRenamed(Session session, CatalogSchemaTableName sourceTable, CatalogSchemaTableName targetTable)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(sourceTable);
        throwIfSystemCatalog(targetTable);
        accessControlClient.entityRenamed(toDispatchSession(session), toTableEntity(sourceTable), toTableEntity(targetTable));
    }

    @Override
    public void tableDropped(Session session, CatalogSchemaTableName table)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        accessControlClient.entityDropped(toDispatchSession(session), toTableEntity(table));
    }

    @Override
    public void columnCreated(Session session, CatalogSchemaTableName table, String column)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        accessControlClient.entityCreated(toDispatchSession(session), toColumnEntity(table, column));
    }

    @Override
    public void columnRenamed(Session session, CatalogSchemaTableName table, String oldName, String newName)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        accessControlClient.entityRenamed(toDispatchSession(session), toColumnEntity(table, oldName), toColumnEntity(table, newName));
    }

    @Override
    public void columnDropped(Session session, CatalogSchemaTableName table, String column)
    {
        // this will never happen but be safe
        throwIfSystemCatalog(table);
        accessControlClient.entityDropped(toDispatchSession(session), toColumnEntity(table, column));
    }

    // Helper methods

    private static GalaxyPrincipal toGalaxyPrincipal(TrinoPrincipal principal)
    {
        switch (principal.getType()) {
            case ROLE:
                return new GalaxyPrincipal(io.starburst.stargate.accesscontrol.client.PrincipalType.ROLE, principal.getName());
            case USER:
                return new GalaxyPrincipal(io.starburst.stargate.accesscontrol.client.PrincipalType.USER, principal.getName());
            default:
                throw new IllegalArgumentException("Unknown Trino PrincipalType in principal " + principal);
        }
    }

    private static TrinoPrincipal toTrinoPrincipal(GalaxyPrincipal principal)
    {
        switch (principal.getType()) {
            case ROLE:
                return new TrinoPrincipal(ROLE, principal.getName());
            case USER:
                return new TrinoPrincipal(USER, principal.getName());
            default:
                throw new IllegalArgumentException("Unknown Galaxy PrincipalType in principal " + principal);
        }
    }

    private static Set<RoleGrant> toTrinoRoleGrants(Set<io.starburst.stargate.accesscontrol.client.RoleGrant> grants)
    {
        return grants.stream()
                .map(grant -> new RoleGrant(toTrinoPrincipal(grant.getGrantee()), grant.getRoleName().getName(), grant.isGrantable()))
                .collect(toImmutableSet());
    }

    private SchemaId toSchemaEntity(CatalogSchemaName schema)
    {
        return new SchemaId(translateCatalogNameToId(schema.getCatalogName()), schema.getSchemaName());
    }

    private TableId toTableEntity(QualifiedObjectName table)
    {
        return new TableId(translateCatalogNameToId(table.getCatalogName()), table.getSchemaName(), table.getObjectName());
    }

    private ColumnId toColumnEntity(QualifiedObjectName table, String columnName)
    {
        return new ColumnId(translateCatalogNameToId(table.getCatalogName()), table.getSchemaName(), table.getObjectName(), columnName);
    }

    private TableId toTableEntity(CatalogSchemaTableName table)
    {
        return new TableId(translateCatalogNameToId(table.getCatalogName()), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName());
    }

    private ColumnId toColumnEntity(CatalogSchemaTableName table, String columnName)
    {
        return new ColumnId(translateCatalogNameToId(table.getCatalogName()), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName(), columnName);
    }

    private CatalogId translateCatalogNameToId(String catalogName)
    {
        if (isSystemCatalog(catalogName)) {
            throw new TrinoException(NOT_SUPPORTED, "System catalog is read-only");
        }
        return catalogIds.getCatalogId(catalogName)
                .orElseThrow(() -> new TrinoException(CATALOG_NOT_FOUND, format("Catalog '%s' does not exist", catalogName)));
    }

    private static io.starburst.stargate.accesscontrol.privilege.Privilege toGalaxyPrivilege(Privilege privilege)
    {
        return requireNonNull(PRIVILEGE_TRANSLATIONS.get(privilege), "Could not find privilege translation");
    }

    private static boolean isSystemCatalog(String catalogName)
    {
        return catalogName.equalsIgnoreCase("system");
    }

    private static void throwIfSystemCatalog(CatalogSchemaTableName table)
    {
        if (isSystemCatalog(table.getCatalogName())) {
            throw new TrinoException(NOT_SUPPORTED, "System catalog is read-only");
        }
    }

    private static void throwIfSystemCatalog(CatalogSchemaName table)
    {
        if (isSystemCatalog(table.getCatalogName())) {
            throw new TrinoException(NOT_SUPPORTED, "System catalog is read-only");
        }
    }
}
