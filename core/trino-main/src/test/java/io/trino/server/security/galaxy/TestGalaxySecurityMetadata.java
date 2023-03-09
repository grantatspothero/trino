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
import io.starburst.stargate.accesscontrol.client.EntityNotFoundException;
import io.starburst.stargate.accesscontrol.client.OperationNotAllowedException;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.server.security.galaxy.GalaxyTestHelper.FEARLESS_LEADER;
import static io.trino.server.security.galaxy.GalaxyTestHelper.LACKEY_FOLLOWER;
import static io.trino.server.security.galaxy.GalaxyTestHelper.PUBLIC;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestGalaxySecurityMetadata
{
    private GalaxyTestHelper helper;
    private GalaxyAccessControl accessControl;
    private GalaxySecurityMetadata metadataApi;
    private RoleId fearlessRoleId;
    private RoleId lackeyRoleId;

    @BeforeClass(alwaysRun = true)
    public void initialize()
            throws Exception
    {
        helper = new GalaxyTestHelper();
        helper.initialize();
        accessControl = helper.getAccessControl();
        metadataApi = helper.getMetadataApi();
        Map<RoleName, RoleId> roles = helper.getAccessController().listEnabledRoles(helper.adminContext());
        fearlessRoleId = requireNonNull(roles.get(new RoleName(FEARLESS_LEADER)), "Didn't find fearless_leader");
        lackeyRoleId = requireNonNull(roles.get(new RoleName(LACKEY_FOLLOWER)), "Didn't find lackey_follower");
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
        metadataApi = null;
        fearlessRoleId = null;
        lackeyRoleId = null;
    }

    @Test
    public void testCreateAndDropRole()
    {
        // Show that "role1" doesn't exist
        assertThat(metadataApi.roleExists(adminSession(), "role1")).isFalse();

        // Show that "role1" also doesn't show up in the active role set
        assertThat(helper.getActiveRoles(adminContext()).keySet()).doesNotContain(new RoleName("role1"));

        // RoleId public doesn't have the privilege to create a role
        assertThatThrownBy(() -> metadataApi.createRole(publicSession(), "role1", Optional.empty()))
                .isInstanceOf(OperationNotAllowedException.class)
                .hasMessage("Operation not allowed: CREATE_ROLE");

        // RoleId admin has the privilege to create a role
        metadataApi.createRole(adminSession(), "role1", Optional.empty());

        // Show that "role1" now exists
        assertThat(metadataApi.roleExists(adminSession(), "role1")).isTrue();

        // Make sure it shows up the active role set
        assertThat(helper.getActiveRoles(adminContext()).keySet()).contains(new RoleName("role1"));

        // RoleId public doesn't have the privilege to drop a role
        assertThatThrownBy(() -> metadataApi.dropRole(publicSession(), "role1"))
                .isInstanceOf(OperationNotAllowedException.class)
                .hasMessage("Operation not allowed: DELETE_ROLE");

        // RoleId admin has the privilege to drop a role
        metadataApi.dropRole(adminSession(), "role1");

        // Show that "role1" is gone
        assertThat(metadataApi.roleExists(adminSession(), "role1")).isFalse();
    }

    @Test
    public void testCreateRole()
    {
        assertThatThrownBy(() -> metadataApi.createRole(publicSession(), "FailedRole", Optional.empty()))
                .isInstanceOf(OperationNotAllowedException.class)
                .hasMessage("Operation not allowed: CREATE_ROLE");
        metadataApi.createRole(adminSession(), "SAChild1", Optional.empty());

        // It exists for all roles
        assertThat(metadataApi.roleExists(adminSession(), "SAChild1")).isTrue();
        assertThat(metadataApi.roleExists(fearlessSession(), "SAChild1")).isTrue();
        assertThat(metadataApi.roleExists(lackeySession(), "SAChild1")).isTrue();

        metadataApi.dropRole(adminSession(), "SaChILd1");
        assertThat(metadataApi.roleExists(adminSession(), "SaChILd1")).isFalse();
    }

    @Test
    public void testDropRole()
    {
        metadataApi.createRole(adminSession(), "SAChild2", Optional.empty());

        // The capitalization of the role name doesn't matter
        assertThat(metadataApi.roleExists(adminSession(), "sAChIlD2")).isTrue();

        assertThatThrownBy(() -> metadataApi.dropRole(publicSession(), "sacHILD2"))
                .isInstanceOf(OperationNotAllowedException.class)
                .hasMessage("Operation not allowed: DELETE_ROLE");
        assertThatThrownBy(() -> metadataApi.dropRole(adminSession(), "FailedRole"))
                .isInstanceOf(EntityNotFoundException.class)
                .hasMessage("Role not found: failedrole");
        metadataApi.dropRole(adminSession(), "SACHILD2");
        assertThat(metadataApi.roleExists(adminSession(), "sachild2")).isFalse();
    }

    @Test
    public void testListRoles()
    {
        for (Session session : allSessions()) {
            assertThat(metadataApi.listRoles(session)).isEqualTo(ImmutableSet.of(ACCOUNT_ADMIN, FEARLESS_LEADER, LACKEY_FOLLOWER, PUBLIC));
        }
    }

    @Test
    public void testListRoleGrants()
    {
        // No roles currently granted to public running public session
        assertThat(getGrantedRoleNames(metadataApi.listRoleGrants(publicSession(), rolePrincipal(PUBLIC)))).containsOnly(PUBLIC);

        // No roles currently granted to public running admin session
        assertThat(getGrantedRoleNames(metadataApi.listRoleGrants(adminSession(), rolePrincipal(PUBLIC)))).containsOnly(PUBLIC);

        // No roles granted to lackey_follower
        assertThat(getGrantedRoleNames(metadataApi.listRoleGrants(adminSession(), rolePrincipal(LACKEY_FOLLOWER)))).containsOnly(PUBLIC);

        // Unknown role principal does not throw an exception
        assertThat(getGrantedRoleNames(metadataApi.listRoleGrants(adminSession(), rolePrincipal("Whackadoodle")))).isEmpty();

        // fearless_leader is granted to accountadmin
        Set<RoleGrant> grants = metadataApi.listRoleGrants(adminSession(), rolePrincipal(ACCOUNT_ADMIN));
        assertThat(getGrantedRoleNames(grants)).containsOnly(FEARLESS_LEADER, LACKEY_FOLLOWER, PUBLIC);
        checkRoleGrant(grants, ACCOUNT_ADMIN, FEARLESS_LEADER, true);

        // lackey_follower is granted to fearless_leader
        grants = metadataApi.listRoleGrants(adminSession(), rolePrincipal(FEARLESS_LEADER));
        assertThat(getGrantedRoleNames(grants)).containsOnly(LACKEY_FOLLOWER, PUBLIC);
        checkRoleGrant(grants, FEARLESS_LEADER, LACKEY_FOLLOWER, false);
    }

    @Test
    public void testListApplicableRoles()
    {
        assertThat(getGrantedRoleNames(metadataApi.listRoleGrants(adminSession(), rolePrincipal("Whackadoodle")))).isEmpty();

        // Verify that applicable roles for accountadmin are fearless_leader and lackey_follower
        Set<RoleGrant> grants = metadataApi.listApplicableRoles(adminSession(), rolePrincipal(ACCOUNT_ADMIN));
        assertThat(getGrantedRoleNames(grants)).containsOnly(FEARLESS_LEADER, LACKEY_FOLLOWER, PUBLIC);
        checkRoleGrant(grants, FEARLESS_LEADER, LACKEY_FOLLOWER, false);
        checkRoleGrant(grants, ACCOUNT_ADMIN, FEARLESS_LEADER, true);

        // Verify that applicable roles for fearless_leader is lackey_follower
        grants = metadataApi.listApplicableRoles(adminSession(), rolePrincipal(FEARLESS_LEADER));
        assertThat(getGrantedRoleNames(grants)).containsOnly(LACKEY_FOLLOWER, PUBLIC);
        checkRoleGrant(grants, FEARLESS_LEADER, LACKEY_FOLLOWER, false);

        // No applicable roles for public or lackey_follower
        assertThat(getGrantedRoleNames(metadataApi.listApplicableRoles(publicSession(), rolePrincipal(PUBLIC)))).containsOnly(PUBLIC);
        assertThat(getGrantedRoleNames(metadataApi.listApplicableRoles(publicSession(), rolePrincipal(LACKEY_FOLLOWER)))).containsOnly(PUBLIC);
    }

    @Test
    public void testListEnabledRoles()
    {
        assertThat(getGrantedRoleNames(metadataApi.listRoleGrants(adminSession(), rolePrincipal("Whackadoodle")))).isEmpty();

        Set<RoleGrant> grants = metadataApi.listApplicableRoles(adminSession(), rolePrincipal(ACCOUNT_ADMIN));
        assertThat(grants.stream().map(RoleGrant::getRoleName).collect(toImmutableSet())).containsOnly(FEARLESS_LEADER, LACKEY_FOLLOWER, PUBLIC);

        assertThat(getGrantedRoleNames(metadataApi.listApplicableRoles(adminSession(), rolePrincipal(PUBLIC)))).containsOnly(PUBLIC);

        // Verify that enabled roles for fearless_leader is itself, lackey_follower and public
        assertThat(metadataApi.listEnabledRoles(context(fearlessRoleId).getIdentity())).containsOnly(FEARLESS_LEADER, LACKEY_FOLLOWER, PUBLIC);
        checkRoleGrant(grants, FEARLESS_LEADER, LACKEY_FOLLOWER, false);

        assertThat(metadataApi.listEnabledRoles(publicContext().getIdentity())).containsOnly(PUBLIC);
        assertThat(metadataApi.listEnabledRoles(lackeyContext().getIdentity())).containsOnly(LACKEY_FOLLOWER, PUBLIC);
    }

    @Test
    public void testSchemaPrivileges()
    {
        String catalogName = helper.getAnyCatalogName();
        String schemaName = "myschema";
        CatalogSchemaName schema = new CatalogSchemaName(catalogName, schemaName);
        CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, new SchemaTableName(schemaName, "a_table"));
        CatalogSchemaTableName whackadoodle = new CatalogSchemaTableName(catalogName, new SchemaTableName("whackadoodle", "b_table"));
        CatalogSchemaName wildcardSchemaName = new CatalogSchemaName(catalogName, "*");

        // No one can create a table in schema whackadoodle
        helper.checkAccessMatching(
                "Access Denied: Cannot create table catalog1.whackadoodle.b_table.*",
                ImmutableList.of(),
                allContexts(),
                context -> accessControl.checkCanCreateTable(context, whackadoodle, ImmutableMap.of()));

        metadataApi.grantSchemaPrivileges(adminSession(), schema, ImmutableSet.of(Privilege.CREATE), rolePrincipal(FEARLESS_LEADER), true);

        // admin and fearless can create a table, but public and lackey can't
        helper.checkAccessMatching(
                "Access Denied: Cannot create table catalog1.myschema.a_table.*",
                ImmutableList.of(adminContext(), fearlessContext()),
                ImmutableList.of(publicContext(), lackeyContext()),
                context -> accessControl.checkCanCreateTable(context, table, ImmutableMap.of()));

        metadataApi.grantSchemaPrivileges(adminSession(), wildcardSchemaName, ImmutableSet.of(Privilege.CREATE), rolePrincipal(FEARLESS_LEADER), true);

        // Now admin and fearless can create a table in schema whackadoodle, but public and lackey_follower can't
        helper.checkAccessMatching(
                "Access Denied: Cannot create table catalog1.whackadoodle.b_table.*",
                ImmutableList.of(adminContext(), fearlessContext()),
                ImmutableList.of(publicContext(), lackeyContext()),
                context -> accessControl.checkCanCreateTable(context, whackadoodle, ImmutableMap.of()));

        metadataApi.revokeSchemaPrivileges(adminSession(), wildcardSchemaName, ImmutableSet.of(Privilege.CREATE), rolePrincipal(FEARLESS_LEADER), false);

        // Again, no one can create a table in schema whackadoodle
        helper.checkAccessMatching(
                "Access Denied: Cannot create table catalog1.whackadoodle.b_table.*",
                ImmutableList.of(),
                allContexts(),
                context -> accessControl.checkCanCreateTable(context, whackadoodle, ImmutableMap.of()));

        metadataApi.revokeSchemaPrivileges(adminSession(), schema, ImmutableSet.of(Privilege.CREATE), rolePrincipal(FEARLESS_LEADER), false);
    }

    @Test
    public void testDenyTableWildcardPrivileges()
    {
        String catalogName = helper.getAnyCatalogName();
        String schemaName = "myschema";
        String tableName = "mytable";
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, schemaTableName);
        QualifiedObjectName wildcardTableName = new QualifiedObjectName(catalogName, schemaName, "*");
        metadataApi.grantTablePrivileges(adminSession(), wildcardTableName, ImmutableSet.of(Privilege.INSERT), rolePrincipal(FEARLESS_LEADER), true);

        // accountadmin and fearless can insert into the table, but public and lackey can't
        helper.checkAccessMatching(
                "Access Denied: Cannot insert into table catalog1.myschema.mytable.*",
                ImmutableList.of(adminContext(), fearlessContext()),
                ImmutableList.of(publicContext(), lackeyContext()),
                context -> accessControl.checkCanInsertIntoTable(context, table));

        // Add a DENY privilege on lackey.
        metadataApi.denyTablePrivileges(adminSession(), wildcardTableName, ImmutableSet.of(Privilege.INSERT), rolePrincipal(LACKEY_FOLLOWER));

        // Now no one can insert into the table, because lackey is granted to fearless, and fearless is granted to admin
        helper.checkAccessMatching(
                "Access Denied: Cannot insert into table catalog1.myschema.mytable.*",
                ImmutableList.of(),
                allContexts(),
                context -> accessControl.checkCanInsertIntoTable(context, table));

        metadataApi.revokeTablePrivileges(adminSession(), wildcardTableName, ImmutableSet.of(Privilege.INSERT), rolePrincipal(LACKEY_FOLLOWER), false);

        // accountadmin and fearless can insert into the table, but public and lackey can't
        helper.checkAccessMatching(
                "Access Denied: Cannot insert into table catalog1.myschema.mytable.*",
                ImmutableList.of(adminContext(), fearlessContext()),
                ImmutableList.of(publicContext(), lackeyContext()),
                context -> accessControl.checkCanInsertIntoTable(context, table));

        // Remove the wildcard grant
        metadataApi.revokeTablePrivileges(adminSession(), wildcardTableName, ImmutableSet.of(Privilege.INSERT), rolePrincipal(FEARLESS_LEADER), false);
    }

    @Test
    public void testTablePrivileges()
    {
        String catalogName = helper.getAnyCatalogName();
        String schemaName = "myschema";
        String tableName = "mytable";
        CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, new SchemaTableName(schemaName, tableName));
        QualifiedObjectName name = new QualifiedObjectName(catalogName, schemaName, tableName);
        List<TablePrivilegeParameters> parametersList = ImmutableList.of(
                new TablePrivilegeParameters(Privilege.INSERT, "insert into table", context -> accessControl.checkCanInsertIntoTable(context, table)),
                new TablePrivilegeParameters(Privilege.SELECT, "select from columns \\[foo\\] in table or view", context -> accessControl.checkCanSelectFromColumns(context, table, ImmutableSet.of("foo"))),
                new TablePrivilegeParameters(Privilege.DELETE, "delete from table", context -> accessControl.checkCanDeleteFromTable(context, table)),
                new TablePrivilegeParameters(Privilege.UPDATE, "update columns \\[foo\\] in table", context -> accessControl.checkCanUpdateTableColumns(context, table, ImmutableSet.of("foo"))));
        for (boolean grantOption : ImmutableList.of(true, false)) {
            for (TablePrivilegeParameters parameters : parametersList) {
                String message = format("Access Denied: Cannot %s catalog1.myschema.mytable.*", parameters.action);

                // Before a privilege is granted, no role has the table privilege
                helper.checkAccessMatching(message, ImmutableList.of(), allContexts(), parameters.consumer);

                // Grant ALLOW on the table privilege to fearless_leader
                metadataApi.grantTablePrivileges(adminSession(), name, ImmutableSet.of(parameters.privilege), rolePrincipal(FEARLESS_LEADER), grantOption);

                // accountadmin and fearless_leader have the privilege but public and lackey_follower don't
                helper.checkAccessMatching(message, ImmutableList.of(adminContext(), fearlessContext()), ImmutableList.of(publicContext(), lackeyContext()), parameters.consumer);

                metadataApi.denyTablePrivileges(adminSession(), name, ImmutableSet.of(parameters.privilege), rolePrincipal(LACKEY_FOLLOWER));

                // Now no one can has the privilege, even acccountadmin.  accountadmin can't because she has no ALLOW privileges, only ownership
                helper.checkAccessMatching(message, ImmutableList.of(), allContexts(), parameters.consumer);

                metadataApi.revokeTablePrivileges(adminSession(), name, ImmutableSet.of(parameters.privilege), rolePrincipal(LACKEY_FOLLOWER), false);

                // Again accountadmin and fearless_leader have the privilege but public and lackey_follower don't
                helper.checkAccessMatching(message, ImmutableList.of(adminContext(), fearlessContext()), ImmutableList.of(publicContext(), lackeyContext()), parameters.consumer);

                metadataApi.revokeTablePrivileges(adminSession(), name, ImmutableSet.of(parameters.privilege), rolePrincipal(FEARLESS_LEADER), false);

                // Again no one has the privilege
                helper.checkAccessMatching(message, ImmutableList.of(), allContexts(), parameters.consumer);
            }
        }
    }

    @Test
    public void testDenyTablePrivileges()
    {
        String catalogName = helper.getAnyCatalogName();
        String schemaName = "myschema";
        String tableName = "mytable";
        CatalogSchemaTableName table = new CatalogSchemaTableName(catalogName, new SchemaTableName(schemaName, tableName));
        QualifiedObjectName name = new QualifiedObjectName(catalogName, schemaName, tableName);
        List<TablePrivilegeParameters> parametersList = ImmutableList.of(
                new TablePrivilegeParameters(Privilege.INSERT, "insert into table", context -> accessControl.checkCanInsertIntoTable(context, table)),
                new TablePrivilegeParameters(Privilege.SELECT, "select from columns \\[foo\\] in table or view", context -> accessControl.checkCanSelectFromColumns(context, table, ImmutableSet.of("foo"))),
                new TablePrivilegeParameters(Privilege.DELETE, "delete from table", context -> accessControl.checkCanDeleteFromTable(context, table)),
                new TablePrivilegeParameters(Privilege.UPDATE, "update columns \\[foo\\] in table", context -> accessControl.checkCanUpdateTableColumns(context, table, ImmutableSet.of("foo"))));
        for (boolean grantOption : ImmutableList.of(true, false)) {
            for (TablePrivilegeParameters parameters : parametersList) {
                String message = format("Access Denied: Cannot %s catalog1.myschema.mytable.*", parameters.action);

                // To start with, no role can perform the action
                helper.checkAccessMatching(message, ImmutableList.of(), allContexts(), parameters.consumer);

                metadataApi.grantTablePrivileges(adminSession(), name, ImmutableSet.of(parameters.privilege), rolePrincipal(FEARLESS_LEADER), grantOption);

                // Now fearless and admin can perform the action, but lackey and public cannot
                helper.checkAccessMatching(message, ImmutableList.of(adminContext(), fearlessContext()), ImmutableList.of(lackeyContext(), publicContext()), parameters.consumer);

                // Add a DENY privilege to lackey, which overrides the fearless allow privilege
                metadataApi.denyTablePrivileges(adminSession(), name, ImmutableSet.of(parameters.privilege), rolePrincipal(LACKEY_FOLLOWER));

                // Now no one can perform the action
                helper.checkAccessMatching(message, ImmutableList.of(), allContexts(), parameters.consumer);

                // Remove lackey's deny privilege
                metadataApi.revokeTablePrivileges(adminSession(), name, ImmutableSet.of(parameters.privilege), rolePrincipal(LACKEY_FOLLOWER), false);

                // Again fearless and admin can perform the action, but lackey and public cannot
                helper.checkAccessMatching(message, ImmutableList.of(adminContext(), fearlessContext()), ImmutableList.of(lackeyContext(), publicContext()), parameters.consumer);

                // Remove fearless's allow privilege
                metadataApi.revokeTablePrivileges(adminSession(), name, ImmutableSet.of(parameters.privilege), rolePrincipal(FEARLESS_LEADER), false);

                // Again no one can perform the action
                helper.checkAccessMatching(message, ImmutableList.of(), allContexts(), parameters.consumer);
            }
        }
    }

    @Test
    public void testListTablePrivileges()
    {
        String catalogName = helper.getAnyCatalogName();
        String schemaName = "myschema";
        String tableName = "mytable";
        QualifiedObjectName name = new QualifiedObjectName(catalogName, schemaName, tableName);
        QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogName, schemaName, tableName);
        assertThat(metadataApi.listTablePrivileges(adminSession(), prefix)).isEmpty();
        assertThat(metadataApi.listTablePrivileges(fearlessSession(), prefix)).isEmpty();
        assertThat(metadataApi.listTablePrivileges(lackeySession(), prefix)).isEmpty();

        // Adding a DENY privilege doesn't change the result, for now DENY privileges are filtered
        metadataApi.denyTablePrivileges(adminSession(), name, ImmutableSet.of(Privilege.INSERT), rolePrincipal(FEARLESS_LEADER));
        assertThat(metadataApi.listTablePrivileges(adminSession(), prefix)).isEmpty();
        assertThat(metadataApi.listTablePrivileges(fearlessSession(), prefix)).isEmpty();
        assertThat(metadataApi.listTablePrivileges(lackeySession(), prefix)).isEmpty();

        // Grant INSERT with grant option
        metadataApi.grantTablePrivileges(adminSession(), name, ImmutableSet.of(Privilege.INSERT), rolePrincipal(FEARLESS_LEADER), true);

        // accountadmin and fearless_leader have the privilege
        ImmutableList.of(adminSession(), fearlessSession()).forEach(session ->
                assertThat(extractPrivileges(metadataApi.listTablePrivileges(session, prefix))).containsExactly(Privilege.INSERT));

        // lackey_follower and public don't have the insert privilege
        ImmutableList.of(lackeySession(), publicSession()).forEach(session ->
                assertThat(metadataApi.listTablePrivileges(session, prefix)).isEmpty());

        // Adding a DENY privilege changes GrantKind to DENY in the existing row.  No rows are returned
        // because Trino's GrantInfo doesn't have a way to represent DENY privileges.
        metadataApi.denyTablePrivileges(adminSession(), name, ImmutableSet.of(Privilege.INSERT), rolePrincipal(FEARLESS_LEADER));
        assertThat(extractPrivileges(metadataApi.listTablePrivileges(fearlessSession(), prefix))).isEmpty();
        assertThat(metadataApi.listTablePrivileges(lackeySession(), prefix)).isEmpty();

        // Change the privilege back to an ALLOW privilege
        metadataApi.grantTablePrivileges(adminSession(), name, ImmutableSet.of(Privilege.INSERT), rolePrincipal(FEARLESS_LEADER), true);

        // Give lackey_follower a DELETE privilege
        metadataApi.grantTablePrivileges(adminSession(), name, ImmutableSet.of(Privilege.DELETE), rolePrincipal(LACKEY_FOLLOWER), true);

        // lackey_follower and fearless_leader both get the DELETE privilege
        assertThat(extractPrivileges(metadataApi.listTablePrivileges(lackeySession(), prefix))).containsOnly(Privilege.DELETE);
        assertThat(extractPrivileges(metadataApi.listTablePrivileges(fearlessSession(), prefix))).containsOnly(Privilege.INSERT, Privilege.DELETE);

        metadataApi.revokeTablePrivileges(adminSession(), name, ImmutableSet.of(Privilege.INSERT), rolePrincipal(FEARLESS_LEADER), false);

        // lackey_follower and fearless_leader now have only the DELETE privilege
        assertThat(extractPrivileges(metadataApi.listTablePrivileges(lackeySession(), prefix))).containsOnly(Privilege.DELETE);
        assertThat(extractPrivileges(metadataApi.listTablePrivileges(fearlessSession(), prefix))).containsOnly(Privilege.DELETE);

        // Revoke lackey's DELETE privilege
        metadataApi.revokeTablePrivileges(adminSession(), name, ImmutableSet.of(Privilege.DELETE), rolePrincipal(LACKEY_FOLLOWER), false);
    }

    private static class TablePrivilegeParameters
    {
        private final Privilege privilege;
        private final String action;
        private final Consumer<SystemSecurityContext> consumer;

        public TablePrivilegeParameters(Privilege privilege, String action, Consumer<SystemSecurityContext> consumer)
        {
            this.privilege = privilege;
            this.action = action;
            this.consumer = consumer;
        }
    }

    private static Set<Privilege> extractPrivileges(Set<GrantInfo> info)
    {
        return info.stream().map(grant -> grant.getPrivilegeInfo().getPrivilege()).collect(toImmutableSet());
    }

    private Session adminSession()
    {
        return helper.adminSession();
    }

    private Session publicSession()
    {
        return helper.publicSession();
    }

    private Session fearlessSession()
    {
        return session(fearlessRoleId);
    }

    private Session lackeySession()
    {
        return session(lackeyRoleId);
    }

    private List<Session> allSessions()
    {
        return ImmutableList.of(adminSession(), fearlessSession(), lackeySession(), publicSession());
    }

    private List<SystemSecurityContext> allContexts()
    {
        return ImmutableList.of(adminContext(), fearlessContext(), lackeyContext(), publicContext());
    }

    private SystemSecurityContext adminContext()
    {
        return helper.adminContext();
    }

    private SystemSecurityContext publicContext()
    {
        return helper.publicContext();
    }

    private SystemSecurityContext fearlessContext()
    {
        return context(fearlessRoleId);
    }

    private SystemSecurityContext lackeyContext()
    {
        return context(lackeyRoleId);
    }

    private SystemSecurityContext context(RoleId roleId)
    {
        return helper.context(roleId);
    }

    private Session session(RoleId roleId)
    {
        return helper.session(roleId);
    }

    private static TrinoPrincipal rolePrincipal(String roleName)
    {
        return new TrinoPrincipal(PrincipalType.ROLE, roleName);
    }

    private static void checkRoleGrant(Set<RoleGrant> grants, String grantee, String roleName, boolean grantOption)
    {
        RoleGrant roleGrant = grants.stream()
                .filter(grant -> grant.getGrantee().getName().equals(grantee) && grant.getRoleName().equals(roleName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(format("Could not find grantee %s and roleName %s in grants %s", grantee, roleName, grants)));
        assertThat(roleGrant.isGrantable()).isEqualTo(grantOption);
    }

    private static Set<String> getGrantedRoleNames(Set<RoleGrant> grants)
    {
        return grants.stream().map(RoleGrant::getRoleName).collect(toImmutableSet());
    }
}
