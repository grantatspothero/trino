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
import io.airlift.log.Logger;
import io.starburst.stargate.accesscontrol.client.CreateEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.RevokeEntityPrivilege;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient.GrantDetails;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.ColumnId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.FunctionId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.SchemaId;
import io.starburst.stargate.id.TableId;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.FullSystemSecurityContext;
import io.trino.server.security.galaxy.GalaxySystemAccessControlConfig.FilterColumnsAcceleration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.ALLOW;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.DENY;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_SCHEMA;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_TABLE;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.EXECUTE;
import static io.trino.server.security.galaxy.GalaxyAccessControl.canSkipListEnabledRoles;
import static io.trino.server.security.galaxy.GalaxyIdentity.toDispatchSession;
import static io.trino.server.security.galaxy.GalaxySecurityMetadata.PRIVILEGE_TRANSLATIONS;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.server.security.galaxy.GalaxyTestHelper.FEARLESS_LEADER;
import static io.trino.server.security.galaxy.GalaxyTestHelper.LACKEY_FOLLOWER;
import static io.trino.server.security.galaxy.GalaxyTestHelper.PUBLIC;
import static io.trino.spi.security.Privilege.DELETE;
import static io.trino.spi.security.Privilege.INSERT;
import static io.trino.spi.security.Privilege.SELECT;
import static io.trino.spi.security.Privilege.UPDATE;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestGalaxyAccessControl
{
    public static final Logger log = Logger.get(TestGalaxyAccessControl.class);

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();
    private static final AtomicInteger SCHEMA_COUNTER = new AtomicInteger();

    private GalaxyTestHelper helper;
    private GalaxyAccessControl accessControl;
    private GalaxySecurityMetadata securityMetadata;
    private TrinoSecurityApi securityApi;
    private RoleId fearlessRoleId;
    private RoleId lackeyRoleId;
    private List<Identity> fearlessAndLackeyIdentities;

    @BeforeClass(alwaysRun = true)
    public void initialize()
            throws Exception
    {
        helper = new GalaxyTestHelper();
        helper.initialize();
        accessControl = helper.getAccessControl();
        securityMetadata = helper.getMetadataApi();
        securityApi = helper.getClient();
        Map<RoleName, RoleId> roles = helper.getAccessController().listEnabledRoles(adminContext().getIdentity());
        fearlessRoleId = requireNonNull(roles.get(new RoleName(FEARLESS_LEADER)), "Didn't find fearless_leader");
        // Role LACKEY_FOLLOWER is granted to FEARLESS_LEADER
        lackeyRoleId = requireNonNull(roles.get(new RoleName(LACKEY_FOLLOWER)), "Didn't find lackey_follower");
        fearlessAndLackeyIdentities = ImmutableList.of(helper.roleNameToIdentity(FEARLESS_LEADER), helper.roleNameToIdentity(LACKEY_FOLLOWER));
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (helper != null) {
            helper.close();
        }
        helper = null;
        accessControl = null;
        securityMetadata = null;
        securityApi = null;
        fearlessRoleId = null;
        lackeyRoleId = null;
        fearlessAndLackeyIdentities = null;
    }

    @Test
    public void testCheckCanViewQueryOwnedBy()
    {
        testInActiveRoleSet("Cannot view query.*", (identity, roleName) -> accessControl.checkCanViewQueryOwnedBy(identity, helper.roleNameToIdentity(roleName)));
    }

    @Test
    public void testFilterViewQueryOwnedBy()
    {
        assertThat(accessControl.filterViewQueryOwnedBy(fearlessContext().getIdentity(), fearlessAndLackeyIdentities)).isEqualTo(fearlessAndLackeyIdentities);
        assertThat(accessControl.filterViewQueryOwnedBy(lackeyContext().getIdentity(), fearlessAndLackeyIdentities)).containsOnly(helper.roleNameToIdentity(LACKEY_FOLLOWER));
        assertThat(accessControl.filterViewQueryOwnedBy(publicContext().getIdentity(), fearlessAndLackeyIdentities)).isEmpty();
    }

    @Test
    public void testCheckCanKillQueryOwnedBy()
    {
        testInActiveRoleSet("Cannot kill query.*", (identity, roleName) -> accessControl.checkCanKillQueryOwnedBy(identity, helper.roleNameToIdentity(roleName)));
    }

    @Test
    public void testCheckCanAccessCatalog()
    {
        String catalogName = helper.getAnyCatalogName();

        // Catalog access is always allowed because it is just a granular check and detailed check are used after it.
        checkAccess("", allContexts(), ImmutableList.of(), context -> {
            if (!accessControl.canAccessCatalog(context, catalogName)) {
                throw new AccessDeniedException("Cannot access catalog: " + catalogName);
            }
        });
    }

    @Test
    public void testFilterCatalogs()
    {
        StaticCatalogResolver catalogResolver = helper.getCatalogResolver();
        // accountadmin can access all the catalogs
        assertThat(accessControl.filterCatalogs(adminContext(), catalogResolver.getCatalogNames())).isEqualTo(catalogResolver.getCatalogNames());

        // But all other roles see none of the catalogs
        for (SystemSecurityContext context : ImmutableList.of(fearlessContext(), lackeyContext(), publicContext())) {
            assertThat(accessControl.filterCatalogs(context, catalogResolver.getCatalogNames())).isEmpty();
        }
        Set<String> usedCatalogNames = new HashSet<>();
        List<String> catalogNamesToShow = ImmutableList.copyOf(catalogResolver.getCatalogNames()).subList(0, 2);

        // Grant a catalog privilege

        for (String catalogName : catalogNamesToShow) {
            usedCatalogNames.add(catalogName);
            CatalogId catalogId = catalogResolver.getCatalogId(Optional.empty(), catalogName).orElseThrow();
            securityApi.addEntityPrivileges(toDispatchSession(adminSession()), catalogId, ImmutableSet.of(new CreateEntityPrivilege(CREATE_SCHEMA, ALLOW, new RoleName(FEARLESS_LEADER), false)));

            // accoundadmin still sees all catalogs
            assertThat(accessControl.filterCatalogs(adminContext(), catalogResolver.getCatalogNames())).isEqualTo(catalogResolver.getCatalogNames());

            // fearless_leader sees all the catalogs for which privileges exist
            assertThat(accessControl.filterCatalogs(fearlessContext(), catalogResolver.getCatalogNames())).isEqualTo(usedCatalogNames);

            // lackey_follower and public get no catalogs
            for (SystemSecurityContext context : ImmutableList.of(lackeyContext(), publicContext())) {
                assertThat(accessControl.filterCatalogs(context, catalogResolver.getCatalogNames())).isEmpty();
            }
        }

        // Remove the catalog privilege
        for (String catalogName : catalogNamesToShow) {
            CatalogId catalogId = catalogResolver.getCatalogId(Optional.empty(), catalogName).orElseThrow();
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), catalogId, ImmutableSet.of(new RevokeEntityPrivilege(CREATE_SCHEMA, new RoleName(FEARLESS_LEADER), false)));
        }

        usedCatalogNames.clear();

        // Grant schema privileges

        List<CatalogSchemaName> usedSchemaNames = new ArrayList<>();
        for (String catalogName : catalogNamesToShow) {
            usedCatalogNames.add(catalogName);
            CatalogSchemaName newName = new CatalogSchemaName(catalogName, newSchemaName());
            usedSchemaNames.add(newName);
            SchemaId schemaId = new SchemaId(catalogResolver.getCatalogId(Optional.empty(), catalogName).orElseThrow(), newName.getSchemaName());
            securityApi.addEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new CreateEntityPrivilege(CREATE_TABLE, ALLOW, new RoleName(FEARLESS_LEADER), false)));

            // accoundadmin still sees all catalogs
            assertThat(accessControl.filterCatalogs(adminContext(), catalogResolver.getCatalogNames())).isEqualTo(catalogResolver.getCatalogNames());

            // fearless_leader sees all the catalogs for which privileges exist
            assertThat(accessControl.filterCatalogs(fearlessContext(), catalogResolver.getCatalogNames())).isEqualTo(usedCatalogNames);

            // lackey_follower and public get no catalogs
            for (SystemSecurityContext context : ImmutableList.of(lackeyContext(), publicContext())) {
                assertThat(accessControl.filterCatalogs(context, catalogResolver.getCatalogNames())).isEmpty();
            }
        }

        // Remove the schema privileges
        for (CatalogSchemaName schemaName : usedSchemaNames) {
            SchemaId schemaId = new SchemaId(catalogResolver.getCatalogId(Optional.empty(), schemaName.getCatalogName()).orElseThrow(), schemaName.getSchemaName());
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new RevokeEntityPrivilege(CREATE_TABLE, new RoleName(FEARLESS_LEADER), false)));
        }

        usedCatalogNames.clear();

        // Grant a table privilege

        List<QualifiedObjectName> tableNames = new ArrayList<>();
        for (String catalogName : catalogNamesToShow) {
            QualifiedObjectName tableName = new QualifiedObjectName(catalogName, newSchemaName(), newTableName());
            tableNames.add(tableName);
            securityMetadata.grantTablePrivileges(adminSession(), tableName, ImmutableSet.of(UPDATE), trinoPrincipal(FEARLESS_LEADER), false);
            usedCatalogNames.add(catalogName);

            // accoundadmin still sees all catalogs
            assertThat(accessControl.filterCatalogs(adminContext(), catalogResolver.getCatalogNames())).isEqualTo(catalogResolver.getCatalogNames());

            // fearless_leader sees all the catalogs for which privileges exist
            assertThat(accessControl.filterCatalogs(fearlessContext(), catalogResolver.getCatalogNames())).isEqualTo(usedCatalogNames);

            // lackey_follower and public get no catalogs
            for (SystemSecurityContext context : ImmutableList.of(lackeyContext(), publicContext())) {
                assertThat(accessControl.filterCatalogs(context, catalogResolver.getCatalogNames())).isEmpty();
            }
        }

        // Remove the table privilege
        for (QualifiedObjectName tableName : tableNames) {
            securityMetadata.revokeTablePrivileges(adminSession(), tableName, ImmutableSet.of(UPDATE), trinoPrincipal(FEARLESS_LEADER), false);
        }
    }

    @Test
    public void testCheckCanCreateSchema()
    {
        StaticCatalogResolver catalogResolver = helper.getCatalogResolver();
        List<String> catalogNames = ImmutableList.copyOf(catalogResolver.getCatalogNames());
        CatalogSchemaName schemaName = new CatalogSchemaName(catalogNames.get(0), newSchemaName());
        SchemaId schemaId = new SchemaId(catalogResolver.getCatalogId(Optional.empty(), schemaName.getCatalogName()).orElseThrow(), schemaName.getSchemaName());
        String message = format("Access Denied: Cannot create schema %s.%s:.*", schemaName.getCatalogName(), schemaName.getSchemaName());
        // Before CREATE_SCHEMA is granted, only account admin role can create schemas
        checkAccessMatching(message,
                ImmutableList.of(adminContext()),
                ImmutableList.of(fearlessContext(), lackeyContext(), publicContext()),
                context -> accessControl.checkCanCreateSchema(context, schemaName, Map.of()));

        // Grant fearless_leader CREATE_SCHEMA
        securityApi.addEntityPrivileges(toDispatchSession(adminSession()), schemaId.getCatalogId(), ImmutableSet.of(new CreateEntityPrivilege(CREATE_SCHEMA, ALLOW, new RoleName(FEARLESS_LEADER), false)));

        try {
            // Now fearless_leader and accountadmin can create schema, but lackey_follower and public cannot create schema
            checkAccessMatching(message,
                    ImmutableList.of(adminContext(), fearlessContext()),
                    ImmutableList.of(lackeyContext(), publicContext()),
                    context -> accessControl.checkCanCreateSchema(context, schemaName, Map.of()));
        }
        finally {
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), schemaId.getCatalogId(), ImmutableSet.of(new RevokeEntityPrivilege(CREATE_SCHEMA, new RoleName(FEARLESS_LEADER), false)));
        }
    }

    @Test
    public void testCheckCanDropSchema()
    {
        withOwnedSchema("Cannot drop schema %s.*", (context, name) -> accessControl.checkCanDropSchema(context, name));
    }

    @Test
    public void testCheckCanRenameSchema()
    {
        BiConsumer<SystemSecurityContext, CatalogSchemaName> consumer = (context, name) -> accessControl.checkCanRenameSchema(context, name, "whackadoodle");
        CatalogSchemaName name = new CatalogSchemaName(helper.getAnyCatalogName(), newSchemaName());
        SchemaId schemaId = new SchemaId(helper.getCatalogId(name.getCatalogName()), name.getSchemaName());
        String schemaString = format("%s.%s", name.getCatalogName(), name.getSchemaName());
        String messageWithTable = format("Access Denied: Cannot rename schema from %s to whackadoodle.*", schemaString);

        // Before the privilege is granted, admin succeeds and all other roles fail
        checkAccessMatching(
                messageWithTable,
                ImmutableList.of(adminContext()),
                ImmutableList.of(publicContext(), fearlessContext(), lackeyContext()),
                context -> consumer.accept(context, name));

        // Grant the privilege to lackey_follower
        securityApi.addEntityPrivileges(toDispatchSession(adminSession()),
                helper.getCatalogId(name.getCatalogName()), ImmutableSet.of(new CreateEntityPrivilege(CREATE_SCHEMA, ALLOW, new RoleName(LACKEY_FOLLOWER), false)));

        // Only accountadmin succeeds because it is the owner
        checkAccessMatching(
                messageWithTable,
                ImmutableList.of(adminContext()),
                ImmutableList.of(publicContext(), fearlessContext(), lackeyContext()),
                context -> consumer.accept(context, name));

        securityMetadata.setSchemaOwner(adminSession(), name, trinoPrincipal(LACKEY_FOLLOWER));

        try {
            // After changing the owner to lackey_follower, all roles except public work
            checkAccessMatching(
                    messageWithTable,
                    ImmutableList.of(adminContext(), fearlessContext(), lackeyContext()),
                    ImmutableList.of(publicContext()),
                    context -> consumer.accept(context, name));
            consumer.accept(lackeyContext(), name);

            // Remove the catalogId's CREATE_SCHEMA grant
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()),
                    helper.getCatalogId(name.getCatalogName()), ImmutableSet.of(new RevokeEntityPrivilege(CREATE_SCHEMA, new RoleName(LACKEY_FOLLOWER), false)));
            // Remove the CREATE_TABLE privilege created as a result of setSchemaOwnership
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new RevokeEntityPrivilege(CREATE_TABLE, new RoleName(LACKEY_FOLLOWER), false)));

            // After removing the grant, admin succeeds because it got CREATE_SCHEMA on the catalog
            // when the catalog was created.
            checkAccessMatching(
                    messageWithTable,
                    ImmutableList.of(adminContext()),
                    ImmutableList.of(fearlessContext(), lackeyContext(), publicContext()), context -> consumer.accept(context, name));
        }
        catch (Throwable e) {
            log.error(e, "Exception in withCatalogPrivilegeAndSchemaOwnership");
            throw e;
        }

        finally {
            // Move the ownership and remove the privilege
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new RevokeEntityPrivilege(CREATE_TABLE, new RoleName(LACKEY_FOLLOWER), false)));
            securityMetadata.setSchemaOwner(adminSession(), name, new TrinoPrincipal(PrincipalType.ROLE, ACCOUNT_ADMIN));
        }
    }

    @Test
    public void testCheckCanSetSchemaAuthorization()
    {
        withOwnedSchema("Cannot set authorization for schema %s to ROLE any.*",
                (context, name) -> accessControl.checkCanSetSchemaAuthorization(context, name, new TrinoPrincipal(PrincipalType.ROLE, "any")));
    }

    @Test
    public void testFilterSchemas()
    {
        // Show that no filtering happens for system schemas
        Set<String> systemSchemas = ImmutableSet.of("metadata");
        assertThat(accessControl.filterSchemas(publicContext(), "system", systemSchemas)).isEqualTo(systemSchemas);

        // Grant CREATE_TABLE on the schema to fearless_leader
        String catalogName = helper.getAnyCatalogName();
        CatalogId catalogId = helper.getCatalogId(catalogName);
        String schemaName = newSchemaName();
        SchemaId schemaId = new SchemaId(catalogId, schemaName);
        securityApi.addEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new CreateEntityPrivilege(CREATE_TABLE, ALLOW, new RoleName(FEARLESS_LEADER), false)));
        assertThat(accessControl.filterSchemas(fearlessContext(), catalogName, ImmutableSet.of(schemaName, newSchemaName()))).containsOnly(schemaName);
        securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new RevokeEntityPrivilege(CREATE_TABLE, new RoleName(FEARLESS_LEADER), false)));
    }

    @Test
    public void testCheckCanShowCreateSchema()
    {
        String catalogName = helper.getAnyCatalogName();
        CatalogId catalogId = helper.getCatalogId(catalogName);
        String schemaName = newSchemaName();
        SchemaId schemaId = new SchemaId(catalogId, schemaName);
        CatalogSchemaName catalogSchemaName = new CatalogSchemaName(catalogName, schemaName);
        securityApi.setEntityOwner(toDispatchSession(adminSession()), schemaId, new RoleName(ACCOUNT_ADMIN));

        // Show that admin can show create schema
        accessControl.checkCanShowCreateSchema(adminContext(), catalogSchemaName);

        // But public cannot
        assertThatThrownBy(() -> accessControl.checkCanShowCreateSchema(publicContext(), catalogSchemaName))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching("Access Denied: Cannot show create schema for.*");
    }

    @Test
    public void testCheckCanShowCreateTable()
    {
        CatalogSchemaTableName name = new CatalogSchemaTableName(helper.getAnyCatalogName(), new SchemaTableName(newSchemaName(), newTableName()));
        String message = format("Access Denied: Cannot show create table for %s.%s.%s.*", name.getCatalogName(), name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName());

        // An unknown catalog name results in denial
        assertThatThrownBy(() -> accessControl.checkCanShowCreateTable(adminContext(), new CatalogSchemaTableName("unknown_catalog", newSchemaName(), newTableName())))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching("Access Denied: Cannot show create table for.*");

        // Only admin role can show create table, because it has manage_security and there are no privileges have been granted on the table
        checkAccessMatching(message,
                ImmutableList.of(adminContext()),
                ImmutableList.of(fearlessContext(), lackeyContext(), publicContext()),
                context -> accessControl.checkCanShowCreateTable(context, name));

        securityMetadata.grantTablePrivileges(adminSession(), toQualifiedObjectName(name), ImmutableSet.of(Privilege.SELECT), trinoPrincipal(FEARLESS_LEADER), false);
        // Now fearless_leader can show create table but lackey_follower and public cannot
        checkAccessMatching(message,
                ImmutableList.of(adminContext(), fearlessContext()),
                ImmutableList.of(lackeyContext(), publicContext()),
                context -> accessControl.checkCanShowCreateTable(context, name));

        securityMetadata.revokeTablePrivileges(adminSession(), toQualifiedObjectName(name), ImmutableSet.of(Privilege.SELECT), trinoPrincipal(FEARLESS_LEADER), false);
    }

    @Test
    public void testCheckCanCreateTable()
    {
        withSchemaCreateTablePrivilege("Cannot create table %s", (context, name) -> accessControl.checkCanCreateTable(context, name, ImmutableMap.of()));
    }

    @Test
    public void testCheckCanDropTable()
    {
        withOwnedTable("Cannot drop table %s", (context, name) -> accessControl.checkCanDropTable(context, name));
    }

    @Test
    public void testCheckCanRenameTable()
    {
        withSchemaCreateTableAndTableOwnership("Cannot rename table from %s to .*", (context, name) -> {
            CatalogSchemaTableName newName = new CatalogSchemaTableName(name.getCatalogName(), name.getSchemaTableName().getSchemaName(), "whackadoodle");
            accessControl.checkCanRenameTable(context, name, newName);
        });
    }

    @Test
    public void testCheckCanSetTableComment()
    {
        withOwnedTable("Cannot comment table to %s", (context, name) -> accessControl.checkCanSetTableComment(context, name));
    }

    @Test
    public void testCheckCanSetColumnComment()
    {
        withOwnedTable("Cannot comment column to %s", (context, name) -> accessControl.checkCanSetColumnComment(context, name));
    }

    @Test
    public void testFilterTables()
    {
        // Show that no filtering happens for system tables
        Set<SchemaTableName> systemTables = ImmutableSet.of(new SchemaTableName("metadata", "catalogs"));
        assertThat(accessControl.filterTables(publicContext(), "system", systemTables)).isEqualTo(systemTables);

        String catalogName = helper.getAnyCatalogName();
        List<SchemaTableName> tablesList = IntStream.range(0, 5)
                .mapToObj(index -> new SchemaTableName(newSchemaName(), newTableName()))
                .collect(toImmutableList());
        Set<SchemaTableName> tablesSet = ImmutableSet.copyOf(tablesList);

        // Initially, only admin (manage_security) can see any of the tables
        assertThat(accessControl.filterTables(adminContext(), catalogName, tablesSet)).isEqualTo(tablesSet);
        for (SystemSecurityContext context : ImmutableList.of(fearlessContext(), lackeyContext(), publicContext())) {
            assertThat(accessControl.filterTables(context, catalogName, ImmutableSet.copyOf(tablesList))).isEmpty();
        }

        for (boolean grantOption : ImmutableList.of(true, false)) {
            Set<SchemaTableName> usedNames = new HashSet<>();
            for (SchemaTableName schema : tablesList.subList(0, 2)) {
                QualifiedObjectName table = new QualifiedObjectName(catalogName, schema.getSchemaName(), schema.getTableName());
                securityMetadata.grantTablePrivileges(adminSession(), table, ImmutableSet.of(DELETE), trinoPrincipal(FEARLESS_LEADER), grantOption);
                usedNames.add(schema);

                // accountadmin and fearless_leader see the tables for which a privilege has been granted
                assertThat(accessControl.filterTables(fearlessContext(), catalogName, tablesSet)).isEqualTo(usedNames);

                // lackey_follower and public see none of the tables
                ImmutableList.of(lackeyContext(), publicContext()).forEach(context ->
                        assertThat(accessControl.filterTables(context, catalogName, tablesSet)).isEmpty());
            }

            for (SchemaTableName schema : tablesList.subList(0, 2)) {
                QualifiedObjectName table = new QualifiedObjectName(catalogName, schema.getSchemaName(), schema.getTableName());
                securityMetadata.revokeTablePrivileges(adminSession(), table, ImmutableSet.of(DELETE), trinoPrincipal(FEARLESS_LEADER), false);
            }
        }
    }

    @Test
    public void testFilterColumns()
    {
        String catalogName = helper.getAnyCatalogName();
        CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, new SchemaTableName(newSchemaName(), newTableName()));
        Set<String> columns = ImmutableSet.of("column1", "column2", "column3");
        withGrantedTablePrivilege(SELECT, LACKEY_FOLLOWER, table, false, () -> {
            assertThat(accessControl.filterColumns(lackeyContext(), table, columns))
                    .isEqualTo(columns);

            // Simulate access to view and base table within single query
            SystemSecurityContext sameQueryAdmin = adminContext();
            SystemSecurityContext sameQueryUser = new SystemSecurityContext(lackeyContext().getIdentity(), sameQueryAdmin.getQueryId(), sameQueryAdmin.getQueryStart());

            // Note: since admin inherits lackeys role, filterColumns(admin) needs to be invoked before doing DENY below
            assertThat(accessControl.filterColumns(sameQueryAdmin, table, columns))
                    .isEqualTo(Set.of("column1", "column2", "column3"));

            ColumnId columnId = securityMetadata.toColumnEntity(Optional.empty(), table, "column2");
            securityMetadata.addEntityPrivilege(adminSession(), columnId, Set.of(SELECT), DENY, trinoPrincipal(LACKEY_FOLLOWER), false);
            try {
                assertThat(accessControl.filterColumns(sameQueryUser, table, columns))
                        .isEqualTo(Set.of("column1", "column3"));
            }
            finally {
                securityMetadata.revokeEntityPrivileges(adminSession(), columnId, Set.of(SELECT), trinoPrincipal(LACKEY_FOLLOWER), false);
            }
        });
    }

    @Test(dataProvider = "testFilterColumnsBulkDataProvider")
    public void testFilterColumnsBulk(FilterColumnsAcceleration mode)
    {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(
                Set.of(new GalaxySecuritySessionProperties(new GalaxySystemAccessControlConfig())),
                CatalogServiceProvider.fail());

        String catalogName = helper.getAnyCatalogName();
        CatalogSchemaTableName table1 = new CatalogSchemaTableName(catalogName, new SchemaTableName(newSchemaName(), newTableName()));
        CatalogSchemaTableName table2 = new CatalogSchemaTableName(catalogName, new SchemaTableName(newSchemaName(), newTableName()));
        CatalogSchemaTableName table3 = new CatalogSchemaTableName(catalogName, new SchemaTableName(newSchemaName(), newTableName()));
        CatalogSchemaTableName table4 = new CatalogSchemaTableName(catalogName, new SchemaTableName(newSchemaName(), newTableName()));

        TableId table1Id = securityMetadata.toTableEntity(Optional.empty(), table1);
        TableId table2Id = securityMetadata.toTableEntity(Optional.empty(), table2);
        TableId table3Id = securityMetadata.toTableEntity(Optional.empty(), table3);
        TableId table4Id = securityMetadata.toTableEntity(Optional.empty(), table4);
        ColumnId table4ColumnId = securityMetadata.toColumnEntity(Optional.empty(), table4, "column4-1");

        TrinoPrincipal user = trinoPrincipal(LACKEY_FOLLOWER);
        SystemSecurityContext userContext = lackeyContext();
        userContext = new FullSystemSecurityContext(
                Session.builder(sessionPropertyManager)
                        .setOriginalIdentity(userContext.getIdentity())
                        .setIdentity(userContext.getIdentity())
                        .setQueryId(userContext.getQueryId())
                        .setSystemProperty(GalaxySecuritySessionProperties.FILTER_COLUMNS_ACCELERATION, mode.name())
                        .build());

        securityMetadata.addEntityPrivilege(adminSession(), table1Id, Set.of(SELECT), ALLOW, user, false);
        securityMetadata.addEntityPrivilege(adminSession(), table2Id, Set.of(SELECT), ALLOW, user, false);
        securityMetadata.addEntityPrivilege(adminSession(), table3Id, Set.of(SELECT), DENY, user, false);
        securityMetadata.addEntityPrivilege(adminSession(), table4Id, Set.of(SELECT), ALLOW, user, false);
        securityMetadata.addEntityPrivilege(adminSession(), table4ColumnId, Set.of(SELECT), DENY, user, false);

        assertThat(accessControl.filterColumns(
                userContext,
                catalogName,
                Map.of(
                        table1.getSchemaTableName(), Set.of("id", "column1-1"),
                        table2.getSchemaTableName(), Set.of("id", "column2-1", "column2-2"),
                        table3.getSchemaTableName(), Set.of("id", "column3-1", "column3-2"),
                        table4.getSchemaTableName(), Set.of("id", "column4-1", "column4-2"))))
                .isEqualTo(Map.of(
                        table1.getSchemaTableName(), Set.of("id", "column1-1"),
                        table2.getSchemaTableName(), Set.of("id", "column2-1", "column2-2"),
                        table3.getSchemaTableName(), Set.of(), // fully denied
                        table4.getSchemaTableName(), Set.of("id", "column4-2"))); // one column denied

        securityMetadata.revokeEntityPrivileges(adminSession(), table1Id, Set.of(SELECT), user, false);
        securityMetadata.revokeEntityPrivileges(adminSession(), table2Id, Set.of(SELECT), user, false);
        securityMetadata.revokeEntityPrivileges(adminSession(), table3Id, Set.of(SELECT), user, false);
        securityMetadata.revokeEntityPrivileges(adminSession(), table4Id, Set.of(SELECT), user, false);
        securityMetadata.revokeEntityPrivileges(adminSession(), table4ColumnId, Set.of(SELECT), user, false);
    }

    @DataProvider
    public static Object[][] testFilterColumnsBulkDataProvider()
    {
        return Stream.of(FilterColumnsAcceleration.values())
                .collect(toDataProvider());
    }

    @Test
    public void testCheckCanAddColumn()
    {
        withOwnedTable("Cannot add a column to table %s", (context, name) -> accessControl.checkCanAddColumn(context, name));
    }

    @Test
    public void testCheckCanDropColumn()
    {
        withOwnedTable("Cannot drop a column from table %s", (context, name) -> accessControl.checkCanDropColumn(context, name));
    }

    @Test
    public void testCheckCanSetTableAuthorization()
    {
        withOwnedTable("Cannot set authorization for table %s to ROLE any", (context, name) ->
                accessControl.checkCanSetTableAuthorization(context, name, new TrinoPrincipal(PrincipalType.ROLE, "any")));
    }

    @Test
    public void testCheckCanRenameColumn()
    {
        withOwnedTable("Cannot rename a column in table %s", (context, name) -> accessControl.checkCanRenameColumn(context, name));
    }

    @Test
    public void testCheckCanSelectFromColumns()
    {
        withTablePrivilege(Privilege.SELECT, "Cannot select from columns \\[foo\\] in table or view.*",
                (context, name) -> accessControl.checkCanSelectFromColumns(context, name, ImmutableSet.of("foo")));
    }

    @Test
    public void testSelectCountStarWithAllDeniedColumns()
    {
        QualifiedObjectName name = new QualifiedObjectName(helper.getAnyCatalogName(), newSchemaName(), newTableName());
        // Grant SELECT to fearless leader, and DENY SELECT to lackey follower
        securityMetadata.grantTablePrivileges(adminSession(), name, ImmutableSet.of(SELECT), new TrinoPrincipal(PrincipalType.ROLE, FEARLESS_LEADER), false);
        securityMetadata.denyTablePrivileges(adminSession(), name, ImmutableSet.of(SELECT), new TrinoPrincipal(PrincipalType.ROLE, LACKEY_FOLLOWER));

        // Now for both roles we checkCanSelectFromColumns give the appropriate error on an empty set of columns
        CatalogSchemaTableName cname = new CatalogSchemaTableName(helper.getAnyCatalogName(), newSchemaName(), newTableName());
        assertThatThrownBy(() -> accessControl.checkCanSelectFromColumns(lackeyContext(), cname, ImmutableSet.of()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot select from columns [] in table or view %s: Relation not found or not allowed".formatted(cname));

        assertThatThrownBy(() -> accessControl.checkCanSelectFromColumns(fearlessContext(), cname, ImmutableSet.of()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot select from columns [] in table or view %s: Relation not found or not allowed".formatted(cname));

        // Clean up
        securityMetadata.revokeTablePrivileges(adminSession(), name, ImmutableSet.of(SELECT), new TrinoPrincipal(PrincipalType.ROLE, LACKEY_FOLLOWER), false);
        securityMetadata.revokeTablePrivileges(adminSession(), name, ImmutableSet.of(SELECT), new TrinoPrincipal(PrincipalType.ROLE, FEARLESS_LEADER), false);
    }

    @Test
    public void testCheckCanInsertIntoTable()
    {
        withTablePrivilege(INSERT, "Cannot insert into table", (context, name) -> accessControl.checkCanInsertIntoTable(context, name));
    }

    @Test
    public void testCheckCanDeleteFromTable()
    {
        withTablePrivilege(DELETE, "Cannot delete from table", (context, name) -> accessControl.checkCanDeleteFromTable(context, name));
    }

    @Test
    public void testCheckCanTruncateTable()
    {
        withTablePrivilege(DELETE, "Cannot truncate table", (context, name) -> accessControl.checkCanTruncateTable(context, name));
    }

    @Test
    public void testCheckCanUpdateTableColumns()
    {
        withTablePrivilege(UPDATE, "Cannot update columns \\[foo\\] in table.*", (context, name) ->
                accessControl.checkCanUpdateTableColumns(context, name, ImmutableSet.of("foo")));
    }

    @Test
    public void testCheckCanCreateView()
    {
        withSchemaCreateTablePrivilege("Cannot create view %s", (context, name) -> accessControl.checkCanCreateView(context, name));
    }

    @Test
    public void testCheckCanRenameView()
    {
        String message = "Cannot rename view from %s to .*";
        CatalogSchemaTableName view = new CatalogSchemaTableName(helper.getAnyCatalogName(), newSchemaName(), newTableName());
        CatalogSchemaTableName newView = new CatalogSchemaTableName(view.getCatalogName(), view.getSchemaTableName().getSchemaName(), "whackadoodle");

        SchemaId schemaId = new SchemaId(helper.getCatalogId(view.getCatalogName()), view.getSchemaTableName().getSchemaName());
        TableId tableId = new TableId(schemaId.getCatalogId(), schemaId.getSchemaName(), view.getSchemaTableName().getTableName());
        String tableString = format("%s.%s.%s", view.getCatalogName(), view.getSchemaTableName().getSchemaName(), view.getSchemaTableName().getTableName());
        String formattedMessage = format(message, tableString);
        String messageWithTable = format("Access Denied: %s", formattedMessage);

        BiConsumer<SystemSecurityContext, CatalogSchemaTableName> consumer = (context, viewName) ->
                accessControl.checkCanRenameView(context, viewName, newView);
        Runnable checkNoAccess = () -> checkAccessMatching(messageWithTable, ImmutableList.of(), allContexts(), context -> consumer.accept(context, view));

        // Before the ownership and privilege grants, no role succeeds
        checkNoAccess.run();

        // Setting ownership of the original view is not sufficient to grant access
        securityMetadata.setViewOwner(adminSession(), view, trinoPrincipal(LACKEY_FOLLOWER));
        checkNoAccess.run();

        // But granting the privilege with or without grantOption is sufficient.  All but public succeed
        securityApi.addEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new CreateEntityPrivilege(CREATE_TABLE, ALLOW, new RoleName(LACKEY_FOLLOWER), true)));
        checkAccessMatching(messageWithTable, ImmutableList.of(adminContext(), fearlessContext(), lackeyContext()), ImmutableList.of(publicContext()), context -> consumer.accept(context, view));

        // Remove the grant
        securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new RevokeEntityPrivilege(CREATE_TABLE, new RoleName(LACKEY_FOLLOWER), false)));

        // After removing the grant, all roles fail
        checkNoAccess.run();

        // Clean up - - remove privilege and ownership
        securityMetadata.setTableOwner(adminSession(), view, trinoPrincipal(ACCOUNT_ADMIN));
        for (TableId oneTableId : ImmutableList.of(tableId, new TableId(tableId.getCatalogId(), newView.getSchemaTableName().getSchemaName(), newView.getSchemaTableName().getTableName()))) {
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), oneTableId, PRIVILEGE_TRANSLATIONS.values().stream()
                    .filter(privilege -> privilege != CREATE_TABLE)
                    .map(privilege -> new RevokeEntityPrivilege(privilege, new RoleName(LACKEY_FOLLOWER), false))
                    .collect(Collectors.toSet()));
        }
    }

    @Test
    public void testCheckCanSetViewAuthorization()
    {
        withOwnedTable("Cannot set authorization for view %s",
                (context, name) -> accessControl.checkCanSetViewAuthorization(context, name, trinoPrincipal(LACKEY_FOLLOWER)));
    }

    @Test
    public void testCheckCanDropView()
    {
        withOwnedTable("Cannot drop view %s",
                (context, name) -> accessControl.checkCanDropView(context, name));
    }

    @Test
    public void testCheckCanCreateViewWithSelectFromColumns()
    {
        String message = "Access Denied: View owner '[^']+' cannot create view that selects from.*";
        CatalogSchemaTableName name = new CatalogSchemaTableName(helper.getAnyCatalogName(), new SchemaTableName(newSchemaName(), newTableName()));
        Consumer<SystemSecurityContext> consumer = context ->
                accessControl.checkCanCreateViewWithSelectFromColumns(context, name, ImmutableSet.of("some_column"));

        // If no privileges access fails for everyone
        checkAccessMatching(message, ImmutableList.of(), allContexts(), consumer);

        // Having the privilege without grantOption is not enough to allow access unless you own the view
        withGrantedTablePrivilege(SELECT, FEARLESS_LEADER, name, false, () ->
                checkAccessMatching(message, ImmutableList.of(adminContext(), fearlessContext()), ImmutableList.of(publicContext(), lackeyContext()), consumer));

        // Having the privilege with grantOption allows access
        withGrantedTablePrivilege(SELECT, LACKEY_FOLLOWER, name, true, () ->
                checkAccessMatching(message, ImmutableList.of(adminContext(), fearlessContext(), lackeyContext()), ImmutableList.of(publicContext()), consumer));
    }

    @Test
    public void testCheckCanCreateMaterializedView()
    {
        withSchemaCreateTablePrivilege("Cannot create materialized view %s", (context, name) -> accessControl.checkCanCreateMaterializedView(context, name, ImmutableMap.of()));
    }

    @Test
    public void testCheckCanRefreshMaterializedView()
    {
        withOwnedTable("Cannot refresh materialized view %s", (context, name) -> accessControl.checkCanRefreshMaterializedView(context, name));
    }

    @Test
    public void testCheckCanSetMaterializedViewProperties()
    {
        withOwnedTable("Cannot set properties of materialized view %s", (context, name) -> accessControl.checkCanSetMaterializedViewProperties(context, name, ImmutableMap.of()));
    }

    @Test
    public void testCheckCanRenameMaterializedView()
    {
        withOwnedTable("Cannot rename materialized view from %s to .*", (context, name) -> accessControl.checkCanRenameMaterializedView(context, name, new CatalogSchemaTableName(name.getCatalogName(), new SchemaTableName(name.getSchemaTableName().getSchemaName(), newTableName()))));
    }

    @Test
    public void testCanDropMaterializedView()
    {
        withOwnedTable("Cannot drop materialized view %s", (context, name) -> accessControl.checkCanDropMaterializedView(context, name));
    }

    @Test
    public void testCheckCanExecuteProcedure()
    {
        // Always allowed
        CatalogSchemaName name = new CatalogSchemaName(helper.getAnyCatalogName(), newSchemaName());
        checkAccess(
                "",
                allContexts(),
                ImmutableList.of(),
                context -> accessControl.checkCanExecuteProcedure(context, new CatalogSchemaRoutineName(name.getCatalogName(), name.getSchemaName(), "foo")));
    }

    @Test
    public void testSystemCatalogReadOnly()
    {
        testReadOnlyCatalog("system", "metadata", "catalogs", "catalog_name");
    }

    @Test
    public void testReadOnlyCatalog()
    {
        // We know that the pre-created catalogs are read-only, so use the pre-created TPCH catalog
        testReadOnlyCatalog("tpch", "sf1", "regions", "name");
    }

    @Test
    public void testUnsupportedMethods()
    {
        SystemSecurityContext context = adminContext();
        Identity identity = context.getIdentity();
        checkNotSupported("Galaxy does not support user impersonation", () -> accessControl.checkCanImpersonateUser(identity, "mickey_mouse@xample.com"));
        checkNotSupported("Galaxy does not support directly reading Trino cluster system information", () -> accessControl.checkCanReadSystemInformation(identity));
        checkNotSupported("Galaxy does not support directly writing Trino cluster system information", () -> accessControl.checkCanWriteSystemInformation(identity));
    }

    @Test
    public void testTableFunctionPrivileges()
    {
        BiConsumer<SystemSecurityContext, CatalogSchemaRoutineName> checkTableFunction = this::checkCanExecuteFunction;

        CatalogSchemaRoutineName function = new CatalogSchemaRoutineName(helper.getAnyCatalogName(), newSchemaName(), "query");
        String message = "Access Denied: Cannot execute function %s.*".formatted(function);
        Consumer<SystemSecurityContext> checkFunction = context -> checkTableFunction.accept(context, function);

        // Show that with no privilege, the check fails for all contexts regardless of the FunctionKind
        checkAccessMatching(message, ImmutableList.of(), allContexts(), context -> checkCanExecuteFunction(context, function));

        withGrantedFunctionPrivilege(adminContext(), FEARLESS_LEADER, function, false, () -> {
            // If fearlessLeader is granted the privilege, fearlessLeader and admin can execute the function, but lackeyFollower and public can't
            checkAccessMatching(message, ImmutableList.of(adminContext(), fearlessContext()), ImmutableList.of(lackeyContext(), publicContext()), checkFunction);

            // If the schema isn't identical to the granted privilege, no access
            CatalogSchemaRoutineName differentFunction = new CatalogSchemaRoutineName(function.getCatalogName(), newSchemaName(), "query");
            checkAccessMatching(
                    "Access Denied: Cannot execute function %s.*".formatted(differentFunction),
                    ImmutableList.of(),
                    allContexts(),
                    context -> checkTableFunction.accept(context, differentFunction));
        });

        // If lackeyFollower is granted the privilege, lackeyFollower, fearlessLeader and admin can execute the function, but public can't
        withGrantedFunctionPrivilege(adminContext(), LACKEY_FOLLOWER, function, false, () -> {
            checkAccessMatching(message, ImmutableList.of(adminContext(), fearlessContext(), lackeyContext()), ImmutableList.of(publicContext()), checkFunction);
        });

        // If public is granted the privilege, all roles can execute the function
        withGrantedFunctionPrivilege(adminContext(), PUBLIC, function, false, () -> {
            checkAccessMatching(message, allContexts(), ImmutableList.of(), checkFunction);
        });
    }

    private void checkCanExecuteFunction(SystemSecurityContext context, CatalogSchemaRoutineName function)
    {
        if (!accessControl.canExecuteFunction(context, function)) {
            throw new AccessDeniedException("Cannot execute function %s.*".formatted(function));
        }
    }

    @Test
    public void testCanSkipSqlOperation()
    {
        ImmutableList.of("SELECT", "SeLecT", "INSERT", "insert", "DELETE", "dElEtE", "UPDATE", "update").forEach(operation ->
                ImmutableList.of(" ", "\t", "(", " (", " [").forEach(suffix ->
                        ImmutableList.of("", " ", "\t").forEach(prefix ->
                                assertThat(canSkipListEnabledRoles("%s%s%sfruit from bats".formatted(prefix, operation, suffix)))
                                        .isTrue())));
        ImmutableList.of("SELECTifyoucan ", "show rolws", "a SELECT FROM troubles", "insert.roles").forEach(query ->
                assertThat(canSkipListEnabledRoles(query)).isFalse());
    }

    private static void checkNotSupported(String message, Runnable consumer)
    {
        assertThatThrownBy(consumer::run)
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching(message + ".*");
    }

    private void testReadOnlyCatalog(String catalogName, String schemaName, String tableName, String columnName)
    {
        CatalogSchemaName schema = new CatalogSchemaName(catalogName, schemaName);
        SystemSecurityContext context = adminContext();

        // Test that read-only schema operations are allowed
        assertThat(accessControl.canAccessCatalog(context, catalogName)).isTrue();
        accessControl.checkCanShowSchemas(context, schema.getCatalogName());
        if (!catalogName.equals("tpch") && !catalogName.equals("system")) {
            // This operation gets an AccessDeniedException for tpch and system because the catalogs
            // are not in the CatalogIds data structure
            // TODO: Figure out how to add the tpch CatalogId to CatalogIds
            accessControl.checkCanShowCreateSchema(context, schema);
        }

        // Show that if the catalog name is unknown, we can't show the schema
        assertThatThrownBy(() -> accessControl.checkCanShowCreateSchema(publicContext(), new CatalogSchemaName("unknown_catalog", newSchemaName())))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching("Access Denied: Cannot show create schema for.*");

        // Test that all schema modify operations are not allowed
        checkNotAllowed(() -> accessControl.checkCanCreateSchema(context, schema, Map.of()), "Cannot create schema");
        checkNotAllowed(() -> accessControl.checkCanDropSchema(context, schema), "Cannot drop schema");
        checkNotAllowed(() -> accessControl.checkCanRenameSchema(context, schema, "rename_will_fail"), "Cannot rename schema");
        checkNotAllowed(() -> accessControl.checkCanSetSchemaAuthorization(context, schema, trinoPrincipal(FEARLESS_LEADER)), "Cannot set authorization for schema");

        CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, new SchemaTableName(schemaName, tableName));
        CatalogSchemaTableName newTable = new CatalogSchemaTableName(catalogName, new SchemaTableName(schemaName, "newtable_" + tableName));
        CatalogSchemaTableName view = new CatalogSchemaTableName(catalogName, schemaName, "view_" + tableName);
        CatalogSchemaTableName newView = new CatalogSchemaTableName(catalogName, schemaName, "newview_" + tableName);

        // Test that read-only table operations are allowed
        accessControl.checkCanShowTables(context, schema);
        if (!catalogName.equals("tpch") && !catalogName.equals("system")) {
            // These operations get an AccessDeniedException for tpch and system because the catalogs
            // aren't in the CatalogIds data structure, and "system" never will be.
            // TODO: Figure out how to add the tpch CatalogId to CatalogIds
            accessControl.checkCanSelectFromColumns(context, table, ImmutableSet.of(columnName));
            accessControl.checkCanShowCreateTable(context, table);
        }

        // Test all table modify operations are not allowed
        checkNotAllowed(() -> accessControl.checkCanCreateTable(context, table, ImmutableMap.of()), "Cannot create table");
        checkNotAllowed(() -> accessControl.checkCanDropTable(context, table), "Cannot drop table");
        checkNotAllowed(() -> accessControl.checkCanRenameTable(context, table, newTable), "Cannot rename table");
        checkNotAllowed(() -> accessControl.checkCanSetTableProperties(context, table, ImmutableMap.of()), "Cannot set table properties");
        checkNotAllowed(() -> accessControl.checkCanSetTableComment(context, table), "Cannot comment table");
        checkNotAllowed(() -> accessControl.checkCanSetColumnComment(context, table), "Cannot comment column");
        checkNotAllowed(() -> accessControl.checkCanAddColumn(context, table), "Cannot add a column");
        checkNotAllowed(() -> accessControl.checkCanDropColumn(context, table), "Cannot drop a column");
        checkNotAllowed(() -> accessControl.checkCanSetTableAuthorization(context, table, trinoPrincipal(FEARLESS_LEADER)), "Cannot set authorization for table");
        checkNotAllowed(() -> accessControl.checkCanRenameColumn(context, table), "Cannot rename a column");
        checkNotAllowed(() -> accessControl.checkCanInsertIntoTable(context, table), "Cannot insert into table");
        checkNotAllowed(() -> accessControl.checkCanDeleteFromTable(context, table), "Cannot delete from table");
        checkNotAllowed(() -> accessControl.checkCanTruncateTable(context, table), "Cannot truncate table");
        checkNotAllowed(() -> accessControl.checkCanUpdateTableColumns(context, table, ImmutableSet.of("foo")), "Cannot update columns");
        checkNotAllowed(() -> accessControl.checkCanCreateView(context, view), "Cannot create view");
        checkNotAllowed(() -> accessControl.checkCanRenameView(context, view, newView), "Cannot rename view");
        checkNotAllowed(() -> accessControl.checkCanSetViewAuthorization(context, view, trinoPrincipal(FEARLESS_LEADER)), "Cannot set authorization for view");
        checkNotAllowed(() -> accessControl.checkCanDropView(context, view), "Cannot drop view");
        checkNotAllowed(() -> accessControl.checkCanExecuteTableProcedure(context, table, "a_procedure"), "Cannot execute procedure");
    }

    private static void checkNotAllowed(Runnable operation, String message)
    {
        assertThatThrownBy(operation::run)
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching(format("Access Denied: %s.*", message));
    }

    private void checkAccess(String message, List<SystemSecurityContext> successfulContexts, List<SystemSecurityContext> failingContexts, Consumer<SystemSecurityContext> consumer)
    {
        helper.checkAccess(message, successfulContexts, failingContexts, consumer);
    }

    private void checkAccessMatching(String message, List<SystemSecurityContext> successfulContexts, List<SystemSecurityContext> failingContexts, Consumer<SystemSecurityContext> consumer)
    {
        helper.checkAccessMatching(message, successfulContexts, failingContexts, consumer);
    }

    private void testInActiveRoleSet(String message, BiConsumer<Identity, String> consumer)
    {
        // The full enumeration of the active role set for each role
        GalaxySystemAccessController controller = helper.getAccessController();
        Map<RoleName, RoleId> lackeyRoleSet = controller.listEnabledRoles(lackeyContext().getIdentity());
        Map<RoleName, RoleId> fearlessRoleSet = controller.listEnabledRoles(fearlessContext().getIdentity());
        Map<RoleName, RoleId> adminRoleSet = controller.listEnabledRoles(adminContext().getIdentity());
        assertThat(lackeyRoleSet.keySet()).containsOnly(new RoleName(PUBLIC), new RoleName(LACKEY_FOLLOWER));
        assertThat(fearlessRoleSet.keySet()).containsOnly(new RoleName(PUBLIC), new RoleName(FEARLESS_LEADER), new RoleName(LACKEY_FOLLOWER));
        assertThat(adminRoleSet.keySet()).containsOnly(new RoleName(PUBLIC), new RoleName(ACCOUNT_ADMIN), new RoleName(FEARLESS_LEADER), new RoleName(LACKEY_FOLLOWER));

        // Show that the consumer accepts the legal cases
        consumer.accept(lackeyContext().getIdentity(), LACKEY_FOLLOWER);
        consumer.accept(lackeyContext().getIdentity(), PUBLIC);
        consumer.accept(fearlessContext().getIdentity(), FEARLESS_LEADER);
        consumer.accept(fearlessContext().getIdentity(), LACKEY_FOLLOWER);
        consumer.accept(fearlessContext().getIdentity(), PUBLIC);

        // Show that the consumer issues the right message if the role is not present in the active role set
        assertThatThrownBy(() -> consumer.accept(lackeyContext().getIdentity(), FEARLESS_LEADER))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching("Access Denied: " + message);

        assertThatThrownBy(() -> consumer.accept(lackeyContext().getIdentity(), FEARLESS_LEADER))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching("Access Denied: " + message);
    }

    private void withOwnedSchema(String message, BiConsumer<SystemSecurityContext, CatalogSchemaName> consumer)
    {
        CatalogSchemaName name = new CatalogSchemaName(helper.getAnyCatalogName(), newSchemaName());
        SchemaId schemaId = new SchemaId(helper.getCatalogId(name.getCatalogName()), name.getSchemaName());
        String schemaString = format("%s.%s", name.getCatalogName(), name.getSchemaName());
        String formattedMessage = format(message, schemaString);
        String messageWithSchema = format("Access Denied: %s", formattedMessage);

        securityMetadata.setSchemaOwner(adminSession(), name, trinoPrincipal(LACKEY_FOLLOWER));

        try {
            // Test that with the roleId that is the direct owner of schema works
            consumer.accept(lackeyContext(), name);

            // Test that with a roleId that is an indirect owner of schema works
            consumer.accept(fearlessContext(), name);

            // Test that with a non-owner fails
            assertThatThrownBy(() -> consumer.accept(publicContext(), name))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching(messageWithSchema);
        }
        finally {
            // Remove schema ownership and CREATE_TABLE privilege
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new RevokeEntityPrivilege(CREATE_TABLE, new RoleName(LACKEY_FOLLOWER), false)));
            securityMetadata.setSchemaOwner(adminSession(), name, new TrinoPrincipal(PrincipalType.ROLE, ACCOUNT_ADMIN));
        }
    }

    private void withOwnedTable(String message, BiConsumer<SystemSecurityContext, CatalogSchemaTableName> consumer)
    {
        CatalogSchemaTableName name = new CatalogSchemaTableName(helper.getAnyCatalogName(), new SchemaTableName(newSchemaName(), newTableName()));
        TableId tableId = new TableId(helper.getCatalogId(name.getCatalogName()), name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName());
        String tableString = format("%s.%s.%s", name.getCatalogName(), name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName());
        String formattedMessage = format(message, tableString);
        String messageWithTable = format("Access Denied: %s", formattedMessage);

        securityMetadata.setTableOwner(adminSession(), name, trinoPrincipal(LACKEY_FOLLOWER));

        try {
            // Test that with the roleId that is the direct owner of table works
            consumer.accept(lackeyContext(), name);

            // Test that with a roleId that is an indirect owner of table works
            consumer.accept(fearlessContext(), name);

            // Test that with a non-owner fails
            assertThatThrownBy(() -> consumer.accept(publicContext(), name))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching(messageWithTable + ".*");
        }
        catch (Throwable e) {
            log.error(e, "Exception in withOwnedTable for name %s", name);
            throw e;
        }
        finally {
            // Remove table ownership and privileges
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), tableId, PRIVILEGE_TRANSLATIONS.values().stream()
                    .filter(privilege -> privilege != CREATE_TABLE)
                    .map(privilege -> new RevokeEntityPrivilege(privilege, new RoleName(LACKEY_FOLLOWER), false))
                    .collect(Collectors.toSet()));
            securityMetadata.setTableOwner(adminSession(), name, new TrinoPrincipal(PrincipalType.ROLE, ACCOUNT_ADMIN));
        }
    }

    private void withSchemaCreateTableAndTableOwnership(String message, BiConsumer<SystemSecurityContext, CatalogSchemaTableName> consumer)
    {
        CatalogSchemaTableName name = new CatalogSchemaTableName(helper.getAnyCatalogName(), newSchemaName(), newTableName());
        SchemaId schemaId = new SchemaId(helper.getCatalogId(name.getCatalogName()), name.getSchemaTableName().getSchemaName());
        TableId tableId = new TableId(schemaId.getCatalogId(), schemaId.getSchemaName(), name.getSchemaTableName().getTableName());
        String tableString = format("%s.%s.%s", name.getCatalogName(), name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName());
        String formattedMessage = format(message, tableString);
        String messageWithTable = format("Access Denied: %s", formattedMessage);

        // Before the privilege is granted, all roles fail
        checkAccessMatching(messageWithTable, ImmutableList.of(), allContexts(), context -> consumer.accept(context, name));

        // Grant the privilege to lackey_follower
        securityApi.addEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new CreateEntityPrivilege(CREATE_TABLE, ALLOW, new RoleName(LACKEY_FOLLOWER), false)));

        // Still only admin succeeds, because the other roles don't have ownership
        checkAccessMatching(
                messageWithTable,
                ImmutableList.of(adminContext()),
                nonAdminContexts(),
                context -> consumer.accept(context, name));

        securityMetadata.setTableOwner(adminSession(), name, trinoPrincipal(LACKEY_FOLLOWER));

        try {
            // After changing the owner to lackey_follower, all roles except public work
            checkAccessMatching(
                    messageWithTable,
                    ImmutableList.of(adminContext(), fearlessContext(), lackeyContext()),
                    ImmutableList.of(publicContext()),
                    context -> consumer.accept(context, name));
            consumer.accept(lackeyContext(), name);

            // Remove the grant
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), schemaId, ImmutableSet.of(new RevokeEntityPrivilege(CREATE_TABLE, new RoleName(LACKEY_FOLLOWER), false)));

            // After removing the grant, all roles fail
            checkAccessMatching(messageWithTable, ImmutableList.of(), allContexts(), context -> consumer.accept(context, name));
        }
        catch (Throwable e) {
            log.error(e, "Exception in withSchemaPrivilegeAndTableOwnership");
            throw e;
        }

        finally {
            // Move the ownership and remove privileges
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), tableId, PRIVILEGE_TRANSLATIONS.values().stream()
                    .filter(privilege -> privilege != CREATE_TABLE)
                    .map(privilege -> new RevokeEntityPrivilege(privilege, new RoleName(LACKEY_FOLLOWER), false))
                    .collect(Collectors.toSet()));
            securityMetadata.setTableOwner(adminSession(), name, new TrinoPrincipal(PrincipalType.ROLE, ACCOUNT_ADMIN));
        }
    }

    private void withSchemaCreateTablePrivilege(String message, BiConsumer<SystemSecurityContext, CatalogSchemaTableName> consumer)
    {
        CatalogSchemaTableName name = new CatalogSchemaTableName(helper.getAnyCatalogName(), new SchemaTableName(newSchemaName(), newTableName()));
        CatalogId catalogId = helper.getCatalogId(name.getCatalogName());
        EntityId schemaEntity = new SchemaId(catalogId, name.getSchemaTableName().getSchemaName());
        String tableString = format("%s.%s.%s", name.getCatalogName(), name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName());
        String formattedMessage = format(message, tableString);
        String messageWithTable = format("Access Denied: %s.*", formattedMessage);

        // Before the privilege is granted, fearless_leader, lackey_follower and public fail
        checkAccessMatching(messageWithTable, ImmutableList.of(), allContexts(), context -> consumer.accept(context, name));

        securityApi.addEntityPrivileges(toDispatchSession(adminSession()), schemaEntity, ImmutableSet.of(new CreateEntityPrivilege(CREATE_TABLE, ALLOW, new RoleName(LACKEY_FOLLOWER), false)));

        try {
            // Test that with the roleId that is the direct or indirect grantee works, but public fails
            List<SystemSecurityContext> contexts = ImmutableList.of(adminContext(), fearlessContext(), lackeyContext());
            checkAccessMatching(messageWithTable, contexts, ImmutableList.of(publicContext()), context -> consumer.accept(context, name));
            consumer.accept(lackeyContext(), name);
        }
        finally {
            // Remove the grant
            securityApi.revokeEntityPrivileges(toDispatchSession(adminSession()), schemaEntity, ImmutableSet.of(new RevokeEntityPrivilege(CREATE_TABLE, new RoleName(LACKEY_FOLLOWER), false)));
        }
    }

    private void withGrantedFunctionPrivilege(SystemSecurityContext context, String roleName, CatalogSchemaRoutineName function, boolean grantOption, Runnable runnable)
    {
        Map<RoleName, RoleId> roles = helper.getActiveRoles(context);
        RoleId roleId = requireNonNull(roles.get(new RoleName(roleName)), "Could not find role " + roleName);
        FunctionId functionId = new FunctionId(helper.getCatalogId(function.getCatalogName()), function.getSchemaName(), function.getRoutineName());
        GrantDetails grantDetails = new GrantDetails(EXECUTE, roleId, ALLOW, grantOption, functionId);
        helper.getAccountClient().grantFunctionPrivilege(grantDetails);
        try {
            runnable.run();
        }
        finally {
            helper.getAccountClient().revokeFunctionPrivilege(grantDetails);
        }
    }

    private void withGrantedTablePrivilege(Privilege privilege, String roleName, CatalogSchemaTableName table, boolean grantOption, Runnable runnable)
    {
        securityMetadata.grantTablePrivileges(adminSession(), toQualifiedObjectName(table), ImmutableSet.of(privilege), trinoPrincipal(roleName), grantOption);
        try {
            runnable.run();
        }
        finally {
            securityMetadata.revokeTablePrivileges(adminSession(), toQualifiedObjectName(table), ImmutableSet.of(privilege), trinoPrincipal(roleName), false);
        }
    }

    private void withTablePrivilege(Privilege privilege, String message, BiConsumer<SystemSecurityContext, CatalogSchemaTableName> consumer)
    {
        CatalogSchemaTableName name = new CatalogSchemaTableName(helper.getAnyCatalogName(), new SchemaTableName(newSchemaName(), newTableName()));
        String messageWithTable = format("Access Denied: %s %s.%s.%s.*", message, name.getCatalogName(), name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName());
        QualifiedObjectName objectName = toQualifiedObjectName(name);

        // Before the privilege is granted, public, fearless_leader and lackey_follower fail
        checkAccessMatching(messageWithTable, ImmutableList.of(), ImmutableList.of(publicContext(), fearlessContext(), lackeyContext()),
                context -> consumer.accept(context, name));

        securityMetadata.grantTablePrivileges(adminSession(), objectName, ImmutableSet.of(privilege), trinoPrincipal(LACKEY_FOLLOWER), false);
        try {
            // accountadmin, fearless_leader and lackey_follower all succeed, and public, which doesn't have the privilege, fails
            checkAccessMatching(messageWithTable, ImmutableList.of(adminContext(), fearlessContext(), lackeyContext()), ImmutableList.of(publicContext()),
                    context -> consumer.accept(context, name));
        }
        finally {
            // Remove the grant
            securityMetadata.revokeTablePrivileges(adminSession(), objectName, ImmutableSet.of(privilege), trinoPrincipal(LACKEY_FOLLOWER), false);
        }

        // After the privilege is revoked, public, fearless_leader and lackey_follower fail
        checkAccessMatching(messageWithTable, ImmutableList.of(), ImmutableList.of(publicContext(), fearlessContext(), lackeyContext()),
                context -> consumer.accept(context, name));

        // Grant a wildcard table privilege on the schema

        QualifiedObjectName schemaWildcard = new QualifiedObjectName(objectName.getCatalogName(), objectName.getSchemaName(), "*");
        securityMetadata.grantTablePrivileges(adminSession(), schemaWildcard, ImmutableSet.of(privilege), trinoPrincipal(LACKEY_FOLLOWER), false);

        try {
            // accountadmin, fearless_leader and lackey_follower all succeed, and public, which doesn't have the privilege, fails
            checkAccessMatching(messageWithTable, ImmutableList.of(adminContext(), fearlessContext(), lackeyContext()), ImmutableList.of(publicContext()),
                    context -> consumer.accept(context, name));

            // Grant a DENY privilege to fearless_leader
            securityMetadata.denyTablePrivileges(adminSession(), objectName, ImmutableSet.of(privilege), trinoPrincipal(FEARLESS_LEADER));

            try {
                // only lackey_follower has the privilege now
                checkAccessMatching(messageWithTable, ImmutableList.of(lackeyContext()), ImmutableList.of(adminContext(), fearlessContext(), publicContext()),
                        context -> consumer.accept(context, name));
            }
            finally {
                securityMetadata.revokeTablePrivileges(adminSession(), objectName, ImmutableSet.of(privilege), trinoPrincipal(FEARLESS_LEADER), false);
            }

            // accountadmin, fearless_leader and lackey_follower all succeed, and public, which doesn't have the privilege, fails
            checkAccessMatching(messageWithTable, ImmutableList.of(adminContext(), fearlessContext(), lackeyContext()), ImmutableList.of(publicContext()),
                    context -> consumer.accept(context, name));
        }
        finally {
            securityMetadata.revokeTablePrivileges(adminSession(), schemaWildcard, ImmutableSet.of(privilege), trinoPrincipal(LACKEY_FOLLOWER), false);
            securityMetadata.revokeTablePrivileges(adminSession(), schemaWildcard, ImmutableSet.of(privilege), trinoPrincipal(FEARLESS_LEADER), false);
        }

        // Grant a wildcard table privilege on the catalog

        QualifiedObjectName catalogWildcard = new QualifiedObjectName(objectName.getCatalogName(), "*", "*");
        securityMetadata.grantTablePrivileges(adminSession(), catalogWildcard, ImmutableSet.of(privilege), trinoPrincipal(LACKEY_FOLLOWER), false);

        try {
            // accountadmin, fearless_leader and lackey_follower all succeed, and public, which doesn't have the privilege, fails
            checkAccessMatching(messageWithTable, ImmutableList.of(adminContext(), fearlessContext(), lackeyContext()), ImmutableList.of(publicContext()),
                    context -> consumer.accept(context, name));

            // Grant a DENY privilege to fearless_leader
            securityMetadata.denyTablePrivileges(adminSession(), objectName, ImmutableSet.of(privilege), trinoPrincipal(FEARLESS_LEADER));

            try {
                // only lackey_follower has the privilege now
                checkAccessMatching(messageWithTable, ImmutableList.of(lackeyContext()), ImmutableList.of(adminContext(), fearlessContext(), publicContext()),
                        context -> consumer.accept(context, name));
            }
            finally {
                securityMetadata.revokeTablePrivileges(adminSession(), objectName, ImmutableSet.of(privilege), trinoPrincipal(FEARLESS_LEADER), false);
            }

            // accountadmin, fearless_leader and lackey_follower all succeed, and public, which doesn't have the privilege, fails
            checkAccessMatching(messageWithTable, ImmutableList.of(adminContext(), fearlessContext(), lackeyContext()), ImmutableList.of(publicContext()),
                    context -> consumer.accept(context, name));
        }
        finally {
            securityMetadata.revokeTablePrivileges(adminSession(), catalogWildcard, ImmutableSet.of(privilege), trinoPrincipal(LACKEY_FOLLOWER), false);
        }
    }

    private Session adminSession()
    {
        return helper.adminSession();
    }

    private List<SystemSecurityContext> allContexts()
    {
        return ImmutableList.of(adminContext(), fearlessContext(), lackeyContext(), publicContext());
    }

    private List<SystemSecurityContext> nonAdminContexts()
    {
        return ImmutableList.of(fearlessContext(), lackeyContext(), publicContext());
    }

    // TODO this is often used with `.getIdentity()` - provide a adminIdentity getter (instead?)
    private SystemSecurityContext adminContext()
    {
        return helper.adminContext();
    }

    // TODO this is often used with `.getIdentity()` - provide a publicIdentity getter (instead?)
    private SystemSecurityContext publicContext()
    {
        return helper.publicContext();
    }

    // TODO this is often used with `.getIdentity()` - provide a fearlessIdentity getter (instead?)
    private SystemSecurityContext fearlessContext()
    {
        return helper.context(fearlessRoleId);
    }

    // TODO this is often used with `.getIdentity()` - provide a lackeyIdentity getter (instead?)
    private SystemSecurityContext lackeyContext()
    {
        return helper.context(lackeyRoleId);
    }

    private static TrinoPrincipal trinoPrincipal(String roleName)
    {
        return new TrinoPrincipal(PrincipalType.ROLE, roleName);
    }

    private static QualifiedObjectName toQualifiedObjectName(CatalogSchemaTableName name)
    {
        return new QualifiedObjectName(name.getCatalogName(), name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName());
    }

    private static String newSchemaName()
    {
        return "myschema" + SCHEMA_COUNTER.incrementAndGet();
    }

    private static String newTableName()
    {
        return "mytable" + TABLE_COUNTER.incrementAndGet();
    }
}
