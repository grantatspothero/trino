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
package io.trino.galaxy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.airlift.testing.TestingClock;
import io.starburst.stargate.accesscontrol.client.ColumnMaskType;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.client.testing.TestUser;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient.GrantDetails;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.CatalogVersion;
import io.starburst.stargate.id.ColumnMaskId;
import io.starburst.stargate.id.EntityKind;
import io.starburst.stargate.id.PolicyId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.RowFilterId;
import io.starburst.stargate.id.TagId;
import io.starburst.stargate.id.Version;
import io.trino.Session;
import io.trino.connector.CatalogProperties;
import io.trino.connector.ConnectorName;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.galaxy.catalogs.GalaxyCatalogArgs;
import io.trino.server.galaxy.catalogs.GalaxyCatalogInfo;
import io.trino.server.galaxy.catalogs.LiveCatalogsTransactionManager;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.PrivilegeInfo;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingGalaxyCatalogInfoSupplier;
import io.trino.transaction.ForTransactionManager;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Clock;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.ALLOW;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.VIEW_ALL_QUERY_HISTORY;
import static io.trino.server.galaxy.catalogs.CatalogVersioningUtils.toCatalogHandle;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.PORTAL;
import static io.trino.server.security.galaxy.GalaxyIdentity.createIdentity;
import static io.trino.server.security.galaxy.GalaxyIdentity.toDispatchSession;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.DELETE;
import static io.trino.spi.security.Privilege.INSERT;
import static io.trino.spi.security.Privilege.SELECT;
import static io.trino.spi.security.Privilege.UPDATE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * You can debug GalaxyQueryRunner queries in both Trino and Stargate portal-server!
 * <p>
 * To run Trino test TestGalaxyQueries, allowing breakpoints in both Trino and portal-server:
 * <ul>
 *     <li>Debug main class LocalTestingTrinoPortalServer in the stargate project.  No args are required.</li>
 *     <li>Debug TestGalaxyQueries with VM argument -DdebugPortal=true</li>
 * </ul>
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestGalaxyQueries
        extends AbstractTestQueryFramework
{
    private static final String MY_SCHEMA = "my_schema";
    private static final String MY_TABLE = "my_table";
    private static final SchemaTableName MY_SCHEMA_TABLE = new SchemaTableName(MY_SCHEMA, MY_TABLE);
    private static final ImmutableSet<PrivilegeInfo> ALL_TABLE_PRIVILEGES = ImmutableSet.of(SELECT, INSERT, UPDATE, DELETE).stream()
            .map(privilege -> new PrivilegeInfo(privilege, true))
            .collect(toImmutableSet());

    private TestingAccountClient testingAccountClient;
    private final TestingGalaxyCatalogInfoSupplier testingGalaxyCatalogInfoSupplier = new TestingGalaxyCatalogInfoSupplier();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> closeAfterClass(new GalaxyCockroachContainer())));
        testingAccountClient = testingAccountFactory.createAccountClient();
        QueryRunner queryRunner = GalaxyQueryRunner.builder("memory", "tiny")
                .setAccountClient(testingAccountClient)
                .setGalaxyCatalogInfoSupplier(testingGalaxyCatalogInfoSupplier)
                .addPlugin(new TpchPlugin())
                .addCatalog("tpch", "tpch", true, ImmutableMap.of())
                .addPlugin(new MemoryPlugin())
                .addCatalog("memory", "memory", false, ImmutableMap.of())
                .setNodeCount(1)
                .build();
        initWithQueryRunner(queryRunner);

        return queryRunner;
    }

    private void initWithQueryRunner(QueryRunner queryRunner)
    {
        queryRunner.execute(format("CREATE SCHEMA %s.%s", "memory", "tiny"));
    }

    @BeforeEach
    public void setFeatureFlags()
    {
        TestingAccountClient accountClient = getTestingAccountClient();
        accountClient.upsertAccountFeatureFlag("COLUMN_MASKS", true);
        accountClient.upsertAccountFeatureFlag("ROW_FILTERS", true);
    }

    @AfterEach
    public void verifyCleanup()
    {
        assertThat(query("SHOW ROLES"))
                .skippingTypesCheck()
                .matches(varcharColumnResult("accountadmin", "public"));
        assertThat(query("SHOW SCHEMAS FROM memory"))
                .skippingTypesCheck()
                .matches(varcharColumnResult("information_schema", "tiny", "default"));
    }

    @Test
    public void validateEverythingIsWorking()
    {
        assertThat(query("SELECT 1234"))
                .matches("VALUES 1234");
    }

    @Test
    public void testGalaxyNotSupported()
    {
        String explicitGrantorNotSupported = "Galaxy does not support creating a role with an explicit grantor";
        assertQueryFails("CREATE ROLE my_role WITH ADMIN ROLE accountadmin", explicitGrantorNotSupported);
        assertQueryFails("CREATE ROLE my_role WITH ADMIN ROLE public", explicitGrantorNotSupported);
        assertQueryFails("CREATE ROLE my_role WITH ADMIN USER \"admin@example.com\"", explicitGrantorNotSupported);
        assertQueryFails("CREATE ROLE my_role WITH ADMIN CURRENT_USER", explicitGrantorNotSupported);

        String grantWithGrantedByNotSupported = "Galaxy does not support GRANT with the GRANTED BY clause";
        assertQueryFails("GRANT accountadmin TO ROLE public GRANTED BY ROLE accountadmin", grantWithGrantedByNotSupported);
        assertQueryFails("GRANT accountadmin TO ROLE public GRANTED BY ROLE public", grantWithGrantedByNotSupported);
        assertQueryFails("GRANT accountadmin TO ROLE public GRANTED BY USER \"admin@example.com\"", grantWithGrantedByNotSupported);
        assertQueryFails("GRANT accountadmin TO ROLE public GRANTED BY CURRENT_USER", grantWithGrantedByNotSupported);

        String revokeWithGrantedByNotSupported = "Galaxy does not support REVOKE with the GRANTED BY clause";
        assertQueryFails("REVOKE accountadmin FROM ROLE public GRANTED BY ROLE accountadmin", revokeWithGrantedByNotSupported);
        assertQueryFails("REVOKE accountadmin FROM ROLE public GRANTED BY ROLE public", revokeWithGrantedByNotSupported);
        assertQueryFails("REVOKE accountadmin FROM ROLE public GRANTED BY USER \"admin@example.com\"", revokeWithGrantedByNotSupported);
        assertQueryFails("REVOKE accountadmin FROM ROLE public GRANTED BY CURRENT_USER", revokeWithGrantedByNotSupported);

        assertUpdate("CREATE SCHEMA memory.unsupported_test");
        assertQueryFails("GRANT CREATE ON SCHEMA memory.unsupported_test TO USER \"admin@example.com\"", "Galaxy only supports a ROLE as a grantee");
        assertQueryFails("REVOKE CREATE ON SCHEMA memory.unsupported_test FROM USER \"admin@example.com\"", "Galaxy only supports a ROLE as a grantee");
        assertQueryFails("ALTER SCHEMA memory.unsupported_test SET AUTHORIZATION USER \"admin@example.com\"", "Galaxy only supports a ROLE as an owner");

        assertUpdate("CREATE TABLE memory.unsupported_test.my_table (my_column varchar)");
        assertQueryFails("GRANT SELECT ON TABLE memory.unsupported_test.my_table TO USER \"admin@example.com\"", "Galaxy only supports a ROLE as a grantee");
        assertQueryFails("REVOKE SELECT ON TABLE memory.unsupported_test.my_table FROM USER \"admin@example.com\"", "Galaxy only supports a ROLE as a grantee");
        assertQueryFails("ALTER TABLE memory.unsupported_test.my_table SET AUTHORIZATION USER \"admin@example.com\"", "Galaxy only supports a ROLE as an owner");

        assertUpdate("DROP TABLE memory.unsupported_test.my_table");
        assertUpdate("DROP SCHEMA memory.unsupported_test");
    }

    @Test
    public void testRoleManagement()
    {
        Session userA = newUserSession(getTestingAccountClient().getPublicRoleId());
        Session userB = newUserSession(getTestingAccountClient().getPublicRoleId());

        verifyCurrentRoles(ImmutableSet.of(), ImmutableSet.of());

        assertUpdate("CREATE ROLE role_x");
        assertUpdate("CREATE ROLE role_y");
        assertUpdate("CREATE ROLE role_z");
        verifyCurrentRoles(ImmutableSet.of("role_x", "role_y", "role_z"), ImmutableSet.of());

        assertUpdate("GRANT role_x, role_y, role_z TO ROLE public");
        verifyCurrentRoles(ImmutableSet.of("role_x", "role_y", "role_z"), ImmutableSet.of("role_x", "role_y", "role_z"));

        // grant is "if not exist"
        assertUpdate("GRANT role_x, role_y, role_z TO ROLE public");

        assertUpdate("REVOKE role_x, role_y, role_z FROM ROLE public");
        verifyCurrentRoles(ImmutableSet.of("role_x", "role_y", "role_z"), ImmutableSet.of());

        // revoke is "if exists"
        assertUpdate("REVOKE role_x, role_y, role_z FROM ROLE public");

        // multiple grants to users and roles
        assertUpdate("GRANT role_x, role_y TO ROLE role_z, USER \"" + userA.getUser() + "\", USER \"" + userB.getUser() + "\"");
        verifyRoleGrants(userA, ImmutableMultimap.<TrinoPrincipal, String>builder()
                .put(new TrinoPrincipal(USER, userA.getUser()), "role_x")
                .put(new TrinoPrincipal(USER, userA.getUser()), "role_y")
                .build());
        verifyUserRoles(userA, ImmutableSet.of("role_x", "role_y"));
        verifyUserRoles(userB, ImmutableSet.of("role_x", "role_y"));

        assertQueryFails("GRANT role_y, role_z TO USER \"" + userA.getUser() + "\", USER \"" + userB.getUser() + "\" WITH ADMIN OPTION",
                "Galaxy only supports a ROLE for GRANT with ADMIN OPTION");

        assertUpdate("DROP ROLE role_x");
        assertUpdate("DROP ROLE role_y");
        assertUpdate("DROP ROLE role_z");
        verifyCurrentRoles(ImmutableSet.of(), ImmutableSet.of());
    }

    private void verifyCurrentRoles(Set<String> extraAdminRoles, Set<String> extraPublicRoles)
    {
        MaterializedResult allRoles = varcharColumnResult(ImmutableSet.<String>builder()
                .add("accountadmin")
                .add("public")
                .addAll(extraAdminRoles)
                .addAll(extraPublicRoles)
                .build());

        MaterializedResult adminRoles = varcharColumnResult(ImmutableSet.<String>builder()
                .add("accountadmin")
                .add("public")
                .addAll(extraAdminRoles)
                .build());

        MaterializedResult publicRoles = varcharColumnResult(ImmutableSet.<String>builder()
                .add("public")
                .addAll(extraPublicRoles)
                .build());

        assertThat(query("SHOW ROLES"))
                .skippingTypesCheck()
                .matches(allRoles);
        assertThat(query(publicSession(), "SHOW ROLES"))
                .skippingTypesCheck()
                .matches(allRoles);

        assertThat(query("SELECT * FROM system.information_schema.roles"))
                .skippingTypesCheck()
                .matches(allRoles);
        assertThat(query(publicSession(), "SELECT * FROM system.information_schema.roles"))
                .skippingTypesCheck()
                .matches(allRoles);
        assertThat(query("SELECT * FROM tpch.information_schema.roles"))
                .skippingTypesCheck()
                .matches(allRoles);
        assertThat(query(publicSession(), "SELECT * FROM tpch.information_schema.roles"))
                .skippingTypesCheck()
                .matches(allRoles);

        assertThat(query("SHOW CURRENT ROLES"))
                .skippingTypesCheck()
                .matches(adminRoles);
        assertThat(query(publicSession(), "SHOW CURRENT ROLES"))
                .skippingTypesCheck()
                .matches(publicRoles);

        assertThat(query("SELECT * FROM system.information_schema.ENABLED_ROLES"))
                .skippingTypesCheck()
                .matches(adminRoles);
        assertThat(query(publicSession(), "SELECT * FROM system.information_schema.ENABLED_ROLES"))
                .skippingTypesCheck()
                .matches(publicRoles);
        assertThat(query("SELECT * FROM tpch.information_schema.ENABLED_ROLES"))
                .skippingTypesCheck()
                .matches(allRoles);
        assertThat(query(publicSession(), "SELECT * FROM tpch.information_schema.enabled_roles"))
                .skippingTypesCheck()
                .matches(publicRoles);

        // role grants only shows the direct grants of the user, and is not transitive
        assertThat(query("SHOW ROLE GRANTS"))
                .skippingTypesCheck()
                .matches(varcharColumnResult("accountadmin", "public"));
        assertThat(query(publicSession(), "SHOW ROLE GRANTS"))
                .skippingTypesCheck()
                .matches(varcharColumnResult("accountadmin", "public"));

        // applicable roles shows all grants visible to the user
        MaterializedResult.Builder rowGrantsBuilder = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row(getSession().getUser(), "USER", "accountadmin", "NO")
                .row(getSession().getUser(), "USER", "public", "NO");
        extraAdminRoles.forEach(roleName -> rowGrantsBuilder.row("accountadmin", "ROLE", roleName, "YES"));
        extraPublicRoles.forEach(roleName -> rowGrantsBuilder.row("public", "ROLE", roleName, "NO"));
        MaterializedResult rowGrants = rowGrantsBuilder.build();

        // role grants are for the user, so both roles show
        assertThat(query("SELECT * FROM system.information_schema.applicable_roles"))
                .skippingTypesCheck()
                .matches(rowGrants);
        assertThat(query(publicSession(), "SELECT * FROM system.information_schema.applicable_roles"))
                .skippingTypesCheck()
                .matches(rowGrants);
        assertThat(query("SELECT * FROM tpch.information_schema.applicable_roles"))
                .skippingTypesCheck()
                .matches(rowGrants);
        assertThat(query(publicSession(), "SELECT * FROM tpch.information_schema.applicable_roles"))
                .skippingTypesCheck()
                .matches(rowGrants);
    }

    private void verifyUserRoles(Session session, Set<String> roles)
    {
        Set<String> allRoles = ImmutableSet.<String>builder()
                .add("public")
                .addAll(roles)
                .build();

        assertThat(query(session, "SHOW ROLE GRANTS"))
                .skippingTypesCheck()
                .matches(varcharColumnResult(allRoles));

        // applicable roles shows all grants visible to the user
        MaterializedResult.Builder rowGrantsBuilder = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row(session.getUser(), "USER", "public", "NO");
        roles.forEach(roleName -> rowGrantsBuilder.row(session.getUser(), "USER", roleName, "NO"));
        MaterializedResult rowGrants = rowGrantsBuilder.build();

        // role grants are for the user, so both roles show
        assertThat(query(session, "SELECT * FROM system.information_schema.applicable_roles"))
                .skippingTypesCheck()
                .matches(rowGrants);
    }

    private void verifyRoleGrants(Session session, ImmutableMultimap<TrinoPrincipal, String> grants)
    {
        Set<String> allRoles = ImmutableSet.<String>builder()
                .add("public")
                .addAll(grants.values())
                .build();

        assertThat(query(session, "SHOW ROLE GRANTS"))
                .skippingTypesCheck()
                .matches(varcharColumnResult(allRoles));

        // applicable roles shows all grants visible to the user
        MaterializedResult.Builder rowGrantsBuilder = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row(session.getUser(), "USER", "public", "NO");
        grants.forEach((grantee, roleName) -> rowGrantsBuilder.row(grantee.getName(), grantee.getType().toString(), roleName, "NO"));
        MaterializedResult rowGrants = rowGrantsBuilder.build();

        // role grants are for the user, so both roles show
        assertThat(query(session, "SELECT * FROM system.information_schema.applicable_roles"))
                .skippingTypesCheck()
                .matches(rowGrants);
    }

    @Test
    public void testSchema()
    {
        verifySchemaVisibility(ImmutableSet.of(), ImmutableSet.of());

        // admin: create schema and is visible
        // public: not visible
        assertUpdate("CREATE SCHEMA memory.my_schema");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());
        assertThat(query("show create schema memory.my_schema"))
                .matches(result -> result.getOnlyValue().toString().contains("AUTHORIZATION ROLE accountadmin"));

        // public: granted on schema makes the schema visible
        assertUpdate("GRANT CREATE ON SCHEMA memory.my_schema TO ROLE public");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of(MY_SCHEMA));

        // public: revoked grant and verify schema is no longer visible
        assertUpdate("REVOKE create ON SCHEMA memory.my_schema FROM ROLE public");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());

        // admin: create table in schema
        // public: schema is not visible
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(), ImmutableSet.of());
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(), ImmutableSet.of());
        assertUpdate("CREATE TABLE memory.my_schema.my_table (my_column bigint)");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);

        // public: granted select on table, so schema is visible
        assertUpdate("GRANT select ON TABLE memory.my_schema.my_table TO ROLE public");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of(MY_SCHEMA));
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ImmutableSet.of(new PrivilegeInfo(SELECT, false)));

        // public: table dropped with all grants, so schema is not visible
        assertUpdate("DROP TABLE memory.my_schema.my_table");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(), ImmutableSet.of());
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(), ImmutableSet.of());

        // public: granted create on schema, so schema is visible
        assertUpdate("GRANT CREATE ON SCHEMA memory.my_schema TO ROLE public");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of(MY_SCHEMA));

        // admin and public: can not create table because of explicit deny
        assertUpdate("DENY CREATE ON SCHEMA memory.my_schema TO ROLE public");
        // TODO catalog visibility of DENY is broken
        // verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());
        assertQueryFails("CREATE TABLE memory.my_schema.anything (my_column bigint)", "Access Denied.*");
        assertQueryFails(publicSession(), "CREATE TABLE memory.my_schema.anything (my_column bigint)", "Access Denied.*");

        // We can't rename the schema since memory connector does not support it

        // public: schema dropped with all grants, so schema is not visible
        assertUpdate("DROP SCHEMA memory.my_schema");
        verifySchemaVisibility(ImmutableSet.of(), ImmutableSet.of());

        // admin: create schema and is visible
        // public: not visible
        assertUpdate("CREATE SCHEMA memory.my_schema");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());

        // admin: admin change owner to public role
        // public: visible
        assertUpdate("ALTER SCHEMA memory.my_schema SET AUTHORIZATION role public");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of(MY_SCHEMA));
        assertThat(query("show create schema memory.my_schema"))
                .matches(result -> result.getOnlyValue().toString().contains("AUTHORIZATION ROLE public"));

        // public: schema dropped with all grants, so schema is not visible
        assertUpdate(publicSession(), "DROP SCHEMA memory.my_schema");
        verifySchemaVisibility(ImmutableSet.of(), ImmutableSet.of());
    }

    private void verifySchemaVisibility(Set<String> extraAdminSchemas, Set<String> extraPublicSchemas)
    {
        Set<String> adminSchemas = ImmutableSet.<String>builder()
                .add("information_schema")
                .add("tiny")
                .add("default")
                .addAll(extraAdminSchemas)
                .build();
        Set<String> publicSchemas;
        if (extraPublicSchemas.isEmpty()) {
            publicSchemas = ImmutableSet.of();
        }
        else {
            publicSchemas = ImmutableSet.<String>builder()
                    .add("information_schema")
                    .addAll(extraPublicSchemas)
                    .build();
        }

        assertThat(query("SHOW CATALOGS"))
                .skippingTypesCheck()
                .matches(varcharColumnResult("system", "tpch", "memory"));
        assertThat(query(publicSession(), "SHOW CATALOGS"))
                .skippingTypesCheck()
                .matches(varcharColumnResult(ImmutableSet.<String>builder()
                        .add("system")
                        .add("tpch")
                        .addAll(extraPublicSchemas.isEmpty() ? ImmutableSet.of() : ImmutableSet.of("memory"))
                        .build()));

        assertThat(query("SHOW SCHEMAS FROM memory"))
                .skippingTypesCheck()
                .matches(varcharColumnResult(adminSchemas));
        assertThat(query(publicSession(), "SHOW SCHEMAS FROM memory"))
                .matches(varcharColumnResult(publicSchemas));

        assertThat(query("SELECT schema_name FROM memory.information_schema.schemata"))
                .skippingTypesCheck()
                .matches(varcharColumnResult(adminSchemas));
        assertThat(query(publicSession(), "SELECT schema_name FROM memory.information_schema.schemata"))
                .matches(varcharColumnResult(publicSchemas));

        assertThat(query("SHOW SCHEMAS FROM memory"))
                .skippingTypesCheck()
                .matches(varcharColumnResult(adminSchemas));
        assertThat(query(publicSession(), "SHOW SCHEMAS FROM memory"))
                .matches(varcharColumnResult(publicSchemas));
    }

    @Test
    public void testTable()
    {
        assertUpdate("CREATE SCHEMA memory.my_schema");
        assertUpdate("CREATE TABLE memory.my_schema.my_table (my_column varchar)");
        verifySchemaVisibility(ImmutableSet.of(MY_SCHEMA), ImmutableSet.of());
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(), ImmutableSet.of());

        // GRANT INSERT
        assertUpdate("INSERT INTO memory.my_schema.my_table (my_column) VALUES ('a')", 1);
        assertQueryFails(publicSession(), "INSERT INTO memory.my_schema.my_table (my_column) VALUES ('b')", "Access Denied.*");

        assertUpdate("GRANT INSERT ON TABLE memory.my_schema.my_table TO ROLE public");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ImmutableSet.of(new PrivilegeInfo(INSERT, false)));

        assertUpdate("INSERT INTO memory.my_schema.my_table (my_column) VALUES ('c')", 1);
        assertUpdate(publicSession(), "INSERT INTO memory.my_schema.my_table (my_column) VALUES ('d')", 1);

        assertThat(query("select * from memory.my_schema.my_table"))
                .matches(varcharColumnResult("a", "c", "d"));
        assertQueryFails(publicSession(), "select * from memory.my_schema.my_table", "Access Denied.*");

        // memory does not support delete, but we can check if the security checks passed using the exception message
        assertQueryFails("DELETE FROM memory.my_schema.my_table where my_column = 'a'", "This connector does not support modifying table rows");
        assertQueryFails(publicSession(), "DELETE FROM memory.my_schema.my_table where  my_column = 'a'", "Access Denied.*");

        assertUpdate("GRANT DELETE ON TABLE memory.my_schema.my_table TO ROLE public");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ImmutableSet.of(new PrivilegeInfo(INSERT, false), new PrivilegeInfo(DELETE, false)));

        assertQueryFails("DELETE FROM memory.my_schema.my_table where my_column = 'a'", "This connector does not support modifying table rows");
        assertQueryFails(publicSession(), "DELETE FROM memory.my_schema.my_table where  my_column = 'a'", "This connector does not support modifying table rows");

        assertThat(query("select * from memory.my_schema.my_table"))
                .matches(varcharColumnResult("a", "c", "d"));

        // GRANT UPDATE
        // memory does not support update, but we can check if the security checks passed using the exception message
        assertQueryFails("UPDATE memory.my_schema.my_table SET my_column = 'changed'", "This connector does not support modifying table rows");
        assertQueryFails(publicSession(), "UPDATE memory.my_schema.my_table SET my_column = 'changed'", "Access Denied.*");

        assertUpdate("GRANT UPDATE ON TABLE memory.my_schema.my_table TO ROLE public");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(MY_SCHEMA_TABLE),
                ImmutableSet.of(new PrivilegeInfo(INSERT, false), new PrivilegeInfo(DELETE, false), new PrivilegeInfo(UPDATE, false)));

        assertQueryFails("UPDATE memory.my_schema.my_table SET my_column = 'changed'", "This connector does not support modifying table rows");
        assertQueryFails(publicSession(), "UPDATE memory.my_schema.my_table SET my_column = 'changed'", "This connector does not support modifying table rows");

        assertThat(query("select * from memory.my_schema.my_table"))
                .matches(varcharColumnResult("a", "c", "d"));

        // GRANT SELECT
        assertQueryFails(publicSession(), "select * from memory.my_schema.my_table", "Access Denied.*");
        assertUpdate("GRANT SELECT ON TABLE memory.my_schema.my_table TO ROLE public");
        assertThat(query(publicSession(), "select * from memory.my_schema.my_table"))
                .matches(varcharColumnResult("a", "c", "d"));

        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(MY_SCHEMA_TABLE),
                ImmutableSet.of(new PrivilegeInfo(INSERT, false), new PrivilegeInfo(DELETE, false), new PrivilegeInfo(UPDATE, false), new PrivilegeInfo(SELECT, false)));

        // REVOKE SELECT
        assertUpdate("REVOKE SELECT ON TABLE memory.my_schema.my_table FROM ROLE public");
        assertQueryFails(publicSession(), "select * from memory.my_schema.my_table", "Access Denied.*");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(MY_SCHEMA_TABLE),
                ImmutableSet.of(new PrivilegeInfo(INSERT, false), new PrivilegeInfo(DELETE, false), new PrivilegeInfo(UPDATE, false)));

        // DENY INSERT
        assertUpdate("DENY INSERT ON TABLE memory.my_schema.my_table TO ROLE public");
        assertQueryFails("INSERT INTO memory.my_schema.my_table (my_column) VALUES ('fail')", "Access Denied.*");
        assertQueryFails(publicSession(), "INSERT INTO memory.my_schema.my_table (my_column) VALUES ('fail')", "Access Denied.*");

        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(MY_SCHEMA_TABLE), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(MY_SCHEMA_TABLE),
                ImmutableSet.of(new PrivilegeInfo(DELETE, false), new PrivilegeInfo(UPDATE, false)));

        // rename table
        assertUpdate("ALTER TABLE memory.my_schema.my_table RENAME TO  memory.my_schema.renamed_table");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(new SchemaTableName(MY_SCHEMA, "renamed_table")), ALL_TABLE_PRIVILEGES);
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(new SchemaTableName(MY_SCHEMA, "renamed_table")),
                ImmutableSet.of(new PrivilegeInfo(DELETE, false), new PrivilegeInfo(UPDATE, false)));

        // change owner
        assertQueryFails(publicSession(), "DROP TABLE memory.my_schema.renamed_table", "Access Denied.*");
        assertUpdate("ALTER TABLE memory.my_schema.renamed_table  SET AUTHORIZATION role public");
        assertUpdate(publicSession(), "DROP TABLE memory.my_schema.renamed_table");
        verifyTableVisibility("accountadmin", getSession(), ImmutableSet.of(), ImmutableSet.of());
        verifyTableVisibility("public", publicSession(), ImmutableSet.of(), ImmutableSet.of());

        assertUpdate("DROP SCHEMA memory.my_schema");
    }

    private void verifyTableVisibility(String roleName, Session session, Set<SchemaTableName> visibleTables, Set<PrivilegeInfo> privileges)
    {
        assertThat(privileges.isEmpty() || !visibleTables.isEmpty())
                .overridingErrorMessage("visible table are required when privileges are provided")
                .isTrue();

        Set<String> tableSchemas = visibleTables.stream()
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableSet());
        for (String adminTableSchema : tableSchemas) {
            assertThat(query(session, "SHOW TABLES FROM memory." + adminTableSchema))
                    .skippingTypesCheck()
                    .matches(varcharColumnResult(visibleTables.stream()
                                    .filter(name -> adminTableSchema.equals(name.getSchemaName()))
                                    .map(SchemaTableName::getTableName)
                                    .collect(toImmutableSet())));
        }

        @Language("SQL")
        String selectAllVisibleTables = "" +
                "SELECT table_schema || '.' || table_name AS name " +
                "FROM memory.information_schema.tables " +
                "WHERE table_schema not in ('information_schema', 'tiny')";

        assertThat(query(session, selectAllVisibleTables))
                .skippingTypesCheck()
                .matches(varcharColumnResult(visibleTables.stream()
                        .map(SchemaTableName::toString)
                        .collect(toImmutableSet())));

        @Language("SQL")
        String selectTableGrants = "" +
                "SELECT privilege_type, is_grantable, grantee_type, grantee, table_schema, table_name " +
                "FROM memory.information_schema.table_privileges " +
                "WHERE table_schema not in ('information_schema', 'tiny') " +
                "  AND grantee = '" + roleName + "'";

        MaterializedResult.Builder rowGrantsBuilder = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR);
        for (SchemaTableName visibleTable : visibleTables) {
            for (PrivilegeInfo privilege : privileges) {
                rowGrantsBuilder.row(
                        privilege.getPrivilege().toString(),
                        privilege.isGrantOption() ? "YES" : "NO",
                        "ROLE",
                        roleName,
                        visibleTable.getSchemaName(),
                        visibleTable.getTableName());
            }
        }
        assertThat(query(session, selectTableGrants))
                .skippingTypesCheck()
                .matches(rowGrantsBuilder.build());
    }

    @Test
    public void testWildcards()
    {
        assertQueryFails("GRANT CREATE ON SCHEMA \"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("DENY CREATE ON SCHEMA \"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("REVOKE CREATE ON SCHEMA \"*\".\"*\" FROM ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");

        assertUpdate("GRANT CREATE ON SCHEMA memory.\"*\" TO ROLE PUBLIC");
        assertUpdate("DENY CREATE ON SCHEMA memory.\"*\" TO ROLE PUBLIC");
        assertUpdate("REVOKE CREATE ON SCHEMA memory.\"*\" FROM ROLE PUBLIC");

        assertQueryFails("GRANT CREATE ON SCHEMA unknown.\"*\" TO ROLE PUBLIC", ".*Catalog 'unknown' does not exist");
        assertQueryFails("DENY CREATE ON SCHEMA unknown.\"*\" TO ROLE PUBLIC", ".*Catalog 'unknown' does not exist");
        assertQueryFails("REVOKE CREATE ON SCHEMA unknown.\"*\" FROM ROLE PUBLIC", ".*Catalog 'unknown' does not exist");

        assertQueryFails("GRANT CREATE ON SCHEMA \"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("DENY CREATE ON SCHEMA \"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("REVOKE CREATE ON SCHEMA \"*\".\"*\" FROM ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");

        assertQueryFails("GRANT SELECT ON TABLE \"*\".\"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("DENY SELECT ON TABLE \"*\".\"*\".\"*\" TO ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");
        assertQueryFails("REVOKE SELECT ON TABLE \"*\".\"*\".\"*\" FROM ROLE PUBLIC", ".*Catalog wildcard is not allowed.*");

        assertUpdate("GRANT SELECT ON TABLE memory.\"*\".\"*\" TO ROLE PUBLIC");
        assertUpdate("DENY SELECT ON TABLE memory.\"*\".\"*\" TO ROLE PUBLIC");
        assertUpdate("REVOKE SELECT ON TABLE memory.\"*\".\"*\" FROM ROLE PUBLIC");

        assertQueryFails("GRANT SELECT ON TABLE unknown.\"*\".\"*\" TO ROLE PUBLIC", ".*Catalog 'unknown' does not exist");
        assertQueryFails("DENY SELECT ON TABLE unknown.\"*\".\"*\" TO ROLE PUBLIC", ".*Catalog 'unknown' does not exist");
        assertQueryFails("REVOKE SELECT ON TABLE unknown.\"*\".\"*\" FROM ROLE PUBLIC", ".*Catalog 'unknown' does not exist");

        assertUpdate("CREATE SCHEMA memory.my_schema");

        assertUpdate("GRANT SELECT ON TABLE memory.my_schema.\"*\" TO ROLE PUBLIC");
        assertUpdate("DENY SELECT ON TABLE memory.my_schema.\"*\" TO ROLE PUBLIC");
        assertUpdate("REVOKE SELECT ON TABLE memory.my_schema.\"*\" FROM ROLE PUBLIC");

        assertQueryFails("GRANT SELECT ON TABLE memory.unknown.\"*\" TO ROLE PUBLIC", ".*Schema 'memory.unknown' does not exist");
        assertQueryFails("DENY SELECT ON TABLE memory.unknown.\"*\" TO ROLE PUBLIC", ".*Schema 'memory.unknown' does not exist");
        assertQueryFails("REVOKE SELECT ON TABLE memory.unknown.\"*\" FROM ROLE PUBLIC", ".*Schema 'memory.unknown' does not exist");

        assertQueryFails("GRANT SELECT ON TABLE unknown.my_schema.\"*\" TO ROLE PUBLIC", ".*Schema 'unknown.my_schema' does not exist");
        assertQueryFails("DENY SELECT ON TABLE unknown.my_schema.\"*\" TO ROLE PUBLIC", ".*Schema 'unknown.my_schema' does not exist");
        assertQueryFails("REVOKE SELECT ON TABLE unknown.my_schema.\"*\" FROM ROLE PUBLIC", ".*Schema 'unknown.my_schema' does not exist");

        assertUpdate("DROP SCHEMA memory.my_schema");
    }

    @Test
    public void testView()
    {
        assertUpdate("CREATE SCHEMA memory.my_schema");

        assertUpdate("CREATE VIEW memory.my_schema.my_view AS SELECT 'value' as my_value");
        assertThat(query("SELECT * FROM memory.my_schema.my_view"))
                .skippingTypesCheck()
                .isEqualTo(MaterializedResult.resultBuilder(getSession(), createVarcharType(5))
                        .row("value")
                        .build());

        assertUpdate("ALTER VIEW memory.my_schema.my_view SET AUTHORIZATION ROLE PUBLIC");
        assertThat(query("SELECT * FROM memory.my_schema.my_view"))
                .skippingTypesCheck()
                .isEqualTo(MaterializedResult.resultBuilder(getSession(), createVarcharType(5))
                        .row("value")
                        .build());

        assertUpdate("DROP VIEW memory.my_schema.my_view");

        assertUpdate("CREATE VIEW memory.my_schema.my_view SECURITY INVOKER AS SELECT 'value' as my_value");
        assertThat(query("SELECT * FROM memory.my_schema.my_view"))
                .skippingTypesCheck()
                .matches(varcharColumnResult("value"));
        assertUpdate("ALTER VIEW memory.my_schema.my_view SET AUTHORIZATION ROLE PUBLIC");
        assertUpdate(publicSession(), "DROP VIEW memory.my_schema.my_view");

        assertUpdate("DROP SCHEMA memory.my_schema");
    }

    @Test
    public void testViewPrivileges()
    {
        assertUpdate("CREATE SCHEMA memory.my_schema");

        assertUpdate("CREATE VIEW memory.my_schema.my_view AS SELECT 'value' as my_value");
        assertThat(query("SELECT * FROM memory.my_schema.my_view"))
                .skippingTypesCheck()
                .isEqualTo(MaterializedResult.resultBuilder(getSession(), createVarcharType(5))
                        .row("value")
                        .build());

        MaterializedResult emptyResult = MaterializedResult.resultBuilder(getSession(), createVarcharType(5)).build();

        // Revoke all privileges to make it easy to identify added ones
        assertUpdate("REVOKE SELECT, UPDATE, INSERT, DELETE ON memory.my_schema.my_view FROM ROLE accountadmin");
        assertThat(query("SHOW GRANTS ON memory.my_schema.my_view")).skippingTypesCheck().matches(emptyResult);

        assertUpdate("CREATE ROLE view_privileges_role");

        for (Privilege privilege : ImmutableList.of(SELECT, UPDATE, INSERT, DELETE)) {
            // Show that we can grant the privilege on the view to a role
            assertUpdate("GRANT %s ON memory.my_schema.my_view TO ROLE view_privileges_role".formatted(privilege));
            assertThat(query("SHOW GRANTS ON memory.my_schema.my_view"))
                    .skippingTypesCheck()
                    .containsAll("SELECT NULL AS VARCHAR, NULL AS VARCHAR, 'view_privileges_role', 'ROLE', 'memory', 'my_schema', 'my_view', '%s', 'NO', NULL AS VARCHAR".formatted(privilege));
            assertUpdate("REVOKE %s ON memory.my_schema.my_view FROM ROLE view_privileges_role".formatted(privilege));

            // Show that we can deny the privilege...
            assertUpdate("DENY %s ON memory.my_schema.my_view TO ROLE view_privileges_role".formatted(privilege));

            // It's there but Trino doesn't show it yet :(
            assertThat(query("SHOW GRANTS ON memory.my_schema.my_view")).skippingTypesCheck().matches(emptyResult);

            // Revoke it
            assertUpdate("REVOKE %s ON memory.my_schema.my_view FROM ROLE view_privileges_role".formatted(privilege));
        }

        // Clean up
        assertUpdate("DROP view memory.my_schema.my_view");
        assertUpdate("DROP schema memory.my_schema");
        assertUpdate("DROP ROLE view_privileges_role");
    }

    @Test
    public void testQuery()
    {
        assertUpdate("CREATE ROLE my_role");
        RoleId myRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("my_role"));
        Session newUserSession = newUserSession(myRoleId);

        String randomValue = UUID.randomUUID().toString();
        getQueryRunner().execute(newUserSession, "SELECT '" + randomValue + "'");
        String queryId = getQueryRunner().execute(newUserSession, "SELECT query_id from system.runtime.queries where query = 'SELECT ''" + randomValue + "'''").getOnlyValue().toString();

        @Language("SQL")
        String findQuerySql = "SELECT 'found' FROM system.runtime.queries WHERE query_id = '" + queryId + "'";
        // user can always see their own query
        assertThat(query(newUserSession, findQuerySql))
                .skippingTypesCheck()
                .matches(varcharColumnResult("found"));
        // admin can see query because it has my_role active
        assertThat(query(findQuerySql))
                .skippingTypesCheck()
                .matches(varcharColumnResult("found"));
        // public can not see query
        assertThat(query(publicSession(), findQuerySql)).returnsEmptyResult();

        // create another user/role, they cannot see the query
        Session anotherUserSession = newUserSession("public");
        assertThat(query(anotherUserSession, findQuerySql)).returnsEmptyResult();

        // grant public special VIEW_ALL_QUERY_HISTORY account privilege, and both public role sessions can see the query
        TestingAccountClient accountClient = getTestingAccountClient();
        GrantDetails queryHistoryGrant = new GrantDetails(VIEW_ALL_QUERY_HISTORY, accountClient.getPublicRoleId(), ALLOW, false, accountClient.getAccountId());
        accountClient.grantAccountPrivilege(queryHistoryGrant);

        assertThat(query(publicSession(), findQuerySql))
                .skippingTypesCheck()
                .matches(varcharColumnResult("found"));
        assertThat(query(anotherUserSession, findQuerySql))
                .skippingTypesCheck()
                .matches(varcharColumnResult("found"));

        // user can always kill their own query
        assertQueryFails(newUserSession, "CALL system.runtime.kill_query(query_id => '" + queryId + "', message => 'test')", "Target query is not running.*");
        // admin can kill query because it has my_role active
        assertQueryFails("CALL system.runtime.kill_query(query_id => '" + queryId + "', message => 'test')", "Target query is not running.*");
        // public role can not kill the query
        assertQueryFails(publicSession(), "CALL system.runtime.kill_query(query_id => '" + queryId + "', message => 'test')", "Access Denied: Cannot kill query.*");
        assertQueryFails(anotherUserSession, "CALL system.runtime.kill_query(query_id => '" + queryId + "', message => 'test')", "Access Denied: Cannot kill query.*");

        accountClient.revokeAccountPrivilege(queryHistoryGrant);
        assertUpdate("DROP ROLE my_role");
    }

    @Test
    public void testDefinerViews()
    {
        assertUpdate("CREATE SCHEMA memory.definer_views");
        assertUpdate("CREATE TABLE memory.definer_views.test_table(col1 VARCHAR, col2 BOOLEAN)");
        assertUpdate("INSERT INTO memory.definer_views.test_table VALUES('test_update', true)", 1);

        MaterializedResult tableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("test_update", true)
                .build();
        assertThat(query("SELECT * FROM memory.definer_views.test_table"))
                .matches(tableContents);

        // Create a view of the table
        assertUpdate("CREATE VIEW memory.definer_views.test_view AS SELECT * FROM memory.definer_views.test_table");

        // Verify by select from the view
        assertThat(query("SELECT * FROM memory.definer_views.test_view"))
                .matches(tableContents);

        // Make a view_user role
        Session viewUserSession = createUserAndGrantedRole("view_user");

        // Show that before a SELECT grant on the view, view_user can't select from the view
        assertThatThrownBy(() -> query(viewUserSession, "SELECT * FROM memory.definer_views.test_view"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Access Denied: Cannot select from columns \\[col2, col1\\] in table or view memory.definer_views.test_view.*");

        // Grant SELECT on the view to the view_user
        assertUpdate("GRANT SELECT ON TABLE memory.definer_views.test_view TO ROLE view_user with GRANT OPTION");

        // When the user assumes role view_user, she can read through the view
        assertThat(query(viewUserSession, "SELECT * FROM memory.definer_views.test_view"))
                .matches(tableContents);

        // Clean up
        assertUpdate("DROP VIEW memory.definer_views.test_view");
        assertUpdate("DROP TABLE memory.definer_views.test_table");
        assertUpdate("DROP SCHEMA memory.definer_views");
        assertUpdate("DROP ROLE view_user");
    }

    @Test
    public void testDefinerViewFunctions()
    {
        assertUpdate("CREATE SCHEMA memory.definer_views");
        assertUpdate("CREATE TABLE memory.definer_views.test_table(col1 VARCHAR, col2 BOOLEAN)");
        assertUpdate("INSERT INTO memory.definer_views.test_table VALUES('test_update', true)", 1);

        MaterializedResult tableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("test_update", true)
                .build();
        assertThat(query("SELECT * FROM memory.definer_views.test_table"))
                .matches(tableContents);

        // Create a view of the table
        assertUpdate("CREATE VIEW memory.definer_views.test_view AS SELECT hamming_distance(col1, 'Test_Update') AS hamming FROM memory.definer_views.test_table");

        // Verify by select from the view, which calls the function
        assertThat(computeActual("SELECT * FROM memory.definer_views.test_view").getOnlyValue()).isEqualTo(2L);

        // Clean up
        assertUpdate("DROP VIEW memory.definer_views.test_view");
        assertUpdate("DROP TABLE memory.definer_views.test_table");
        assertUpdate("DROP SCHEMA memory.definer_views");
    }

    @Test
    public void testViewGrantOptions()
    {
        MaterializedResult tableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("test_update", true)
                .build();

        assertUpdate("CREATE SCHEMA memory.definer_views");
        Session baseTableOwnerSession = createUserAndGrantedRole("base_table_owner");
        Session innerViewOwnerSessionNoGrantOption = createUserAndGrantedRole("inner_view_owner_no_grant_option");
        Session outerViewOwnerSession = createUserAndGrantedRole("outer_view_owner");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE base_table_owner");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE inner_view_owner_no_grant_option");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE outer_view_owner");

        // Create the base table, insert a row and verify the contents
        assertUpdate(baseTableOwnerSession, "CREATE TABLE memory.definer_views.base_table(col1 VARCHAR, col2 BOOLEAN)");
        assertUpdate(baseTableOwnerSession, "INSERT INTO memory.definer_views.base_table VALUES('test_update', true)", 1);
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.base_table")).matches(tableContents);

        // originally, the views cannot be created
        assertThatThrownBy(() -> query(innerViewOwnerSessionNoGrantOption, "CREATE VIEW memory.definer_views.inner_view_no_grant_option_definer AS SELECT * FROM memory.definer_views.base_table"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot select from columns [col2, col1] in table or view memory.definer_views.base_table: Relation not found or not allowed");
        assertThatThrownBy(() -> query(innerViewOwnerSessionNoGrantOption, "CREATE VIEW memory.definer_views.inner_view_no_grant_option_invoker SECURITY INVOKER AS SELECT * FROM memory.definer_views.base_table"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot select from columns [col2, col1] in table or view memory.definer_views.base_table: Relation not found or not allowed");

        // grant select without the grant option
        assertUpdate(baseTableOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE inner_view_owner_no_grant_option");

        // The views are able to be created without the grant option
        assertUpdate(innerViewOwnerSessionNoGrantOption, "CREATE VIEW memory.definer_views.inner_view_no_grant_option_definer AS SELECT * FROM memory.definer_views.base_table");
        assertUpdate(innerViewOwnerSessionNoGrantOption, "CREATE VIEW memory.definer_views.inner_view_no_grant_option_invoker SECURITY INVOKER AS SELECT * FROM memory.definer_views.base_table");

        // they are also allowed to be queried when the view owner queries them
        assertThat(query(innerViewOwnerSessionNoGrantOption, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer")).matches(tableContents);
        assertThat(query(innerViewOwnerSessionNoGrantOption, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker")).matches(tableContents);

        // however, without the grant option, another role won't be able to query the view
        assertThatThrownBy(() -> query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot select from columns [col2, col1] in table or view memory.definer_views.inner_view_no_grant_option_definer: Relation not found or not allowed");
        assertThatThrownBy(() -> query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot select from columns [col2, col1] in table or view memory.definer_views.inner_view_no_grant_option_invoker: Relation not found or not allowed");

        // give other role SELECT on the view, and the views will still fail:
        //  - definer view requires inner_view_owner_no_grant_option have SELECT WITH GRANT OPTION on the underlying table
        //  - invoker view requires outer_view_owner have SELECT on the underlying table
        assertUpdate(innerViewOwnerSessionNoGrantOption, "GRANT SELECT ON TABLE memory.definer_views.inner_view_no_grant_option_definer TO ROLE outer_view_owner");
        assertUpdate(innerViewOwnerSessionNoGrantOption, "GRANT SELECT ON TABLE memory.definer_views.inner_view_no_grant_option_invoker TO ROLE outer_view_owner");
        assertThatThrownBy(() -> query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Access Denied: View owner does not have sufficient privileges")
                .hasMessageContaining("cannot create view that selects from memory.definer_views.base_table: Role outer_view_owner does not have the privilege SELECT WITH GRANT OPTION on the columns");
        assertThatThrownBy(() -> query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot select from columns [col2, col1] in table or view memory.definer_views.base_table: Relation not found or not allowed");

        // grant with grant option, and the definer view can now be queried, but invoker still needs the grant
        assertUpdate(baseTableOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE inner_view_owner_no_grant_option WITH GRANT OPTION");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer")).matches(tableContents);
        assertThatThrownBy(() -> query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot select from columns [col2, col1] in table or view memory.definer_views.base_table: Relation not found or not allowed");

        // give base table access to outer_view_owner, and now querying should work for outer_view_owner role
        assertUpdate(baseTableOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE outer_view_owner");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer")).matches(tableContents);
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker")).matches(tableContents);

        // make sure this doesn't work for base_table_owner, as they don't have view privileges
        assertThatThrownBy(() -> query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot select from columns [col2, col1] in table or view memory.definer_views.inner_view_no_grant_option_definer: Relation not found or not allowed");
        assertThatThrownBy(() -> query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot select from columns [col2, col1] in table or view memory.definer_views.inner_view_no_grant_option_invoker: Relation not found or not allowed");

        // grant inner_view_owner_no_grant_option to base_table_owner role and querying the views should succeed
        assertUpdate("GRANT inner_view_owner_no_grant_option TO ROLE base_table_owner");
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer")).matches(tableContents);
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker")).matches(tableContents);

        // revoke the grant option -- now querying by outer_view_owner should fail again,
        // but base_table_owner should still succeed, as it has the view owner in its active role set
        assertUpdate(baseTableOwnerSession, "REVOKE SELECT ON TABLE memory.definer_views.base_table FROM ROLE inner_view_owner_no_grant_option");
        assertUpdate(baseTableOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE inner_view_owner_no_grant_option");

        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer")).matches(tableContents);
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker")).matches(tableContents);

        assertThatThrownBy(() -> query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Access Denied: View owner does not have sufficient privileges")
                .hasMessageContaining("cannot create view that selects from memory.definer_views.base_table: Role outer_view_owner does not have the privilege SELECT WITH GRANT OPTION on the columns");
        // invoker is still good
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_invoker")).matches(tableContents);

        // revoke the role grant and the definer view isn't accessible to either role anymore
        assertUpdate("REVOKE inner_view_owner_no_grant_option FROM ROLE base_table_owner");
        assertThatThrownBy(() -> query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot select from columns [col2, col1] in table or view memory.definer_views.inner_view_no_grant_option_definer: Relation not found or not allowed");
        assertThatThrownBy(() -> query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view_no_grant_option_definer"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Access Denied: View owner does not have sufficient privileges")
                .hasMessageContaining("cannot create view that selects from memory.definer_views.base_table: Role outer_view_owner does not have the privilege SELECT WITH GRANT OPTION on the columns");

        // Clean up
        assertUpdate("DROP VIEW memory.definer_views.inner_view_no_grant_option_invoker");
        assertUpdate("DROP VIEW memory.definer_views.inner_view_no_grant_option_definer");
        assertUpdate("DROP TABLE memory.definer_views.base_table");
        assertUpdate("DROP SCHEMA memory.definer_views");
        assertUpdate("DROP ROLE outer_view_owner");
        assertUpdate("DROP ROLE inner_view_owner_no_grant_option");
        assertUpdate("DROP ROLE base_table_owner");
    }

    @Test
    public void testNestedViews()
    {
        MaterializedResult tableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("test_update", true)
                .build();

        assertUpdate("CREATE SCHEMA memory.definer_views");
        Session baseTableOwnerSession = createUserAndGrantedRole("base_table_owner");
        Session innerViewOwnerSession = createUserAndGrantedRole("inner_view_owner");
        Session outerViewOwnerSession = createUserAndGrantedRole("outer_view_owner");
        Session testRoleSession = createUserAndGrantedRole("test_role");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE base_table_owner");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE inner_view_owner");
        assertUpdate("GRANT CREATE ON schema definer_views TO ROLE outer_view_owner");

        // Create the base table, insert a row and verify the contents
        assertUpdate(baseTableOwnerSession, "CREATE TABLE memory.definer_views.base_table(col1 VARCHAR, col2 BOOLEAN)");
        assertUpdate(baseTableOwnerSession, "INSERT INTO memory.definer_views.base_table VALUES('test_update', true)", 1);
        assertThat(query(baseTableOwnerSession, "SELECT * FROM memory.definer_views.base_table")).matches(tableContents);

        assertUpdate(baseTableOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE inner_view_owner WITH GRANT OPTION");

        // Create the inner view and verify contents
        assertUpdate(innerViewOwnerSession, "CREATE VIEW memory.definer_views.inner_view AS SELECT * FROM memory.definer_views.base_table");
        assertThat(query(innerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view")).matches(tableContents);

        // Grant select on inner_view to role outer_view_owner with grant option
        assertUpdate(innerViewOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.inner_view TO ROLE outer_view_owner WITH GRANT OPTION");

        // Create the outer view and verify contents
        assertUpdate(outerViewOwnerSession, "CREATE VIEW memory.definer_views.outer_view AS SELECT * FROM memory.definer_views.inner_view");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.outer_view")).matches(tableContents);

        // Verify that outer_view_owner cannot access base_table
        assertThatThrownBy(() -> query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.base_table"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Access Denied.*");

        // Grant SELECT on outer_view to test_role and verify contents
        assertUpdate(outerViewOwnerSession, "GRANT SELECT ON TABLE memory.definer_views.outer_view TO ROLE test_role");
        assertThat(query(testRoleSession, "SELECT * FROM memory.definer_views.outer_view")).matches(tableContents);

        // Verify that test_role cannot access either inner_view or base_table
        for (String tableName : ImmutableList.of("inner_view", "base_table")) {
            assertThatThrownBy(() -> query(testRoleSession, "SELECT * FROM memory.definer_views." + tableName))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching("Access Denied.*");
        }

        // Revoke the SELECT grant to inner_view_owner on the base_table and verify that
        // neither inner_view_owner or outer_view_owner or test_role can access the view.
        assertUpdate(innerViewOwnerSession, "REVOKE SELECT ON TABLE memory.definer_views.base_table FROM ROLE inner_view_owner");
        assertThatThrownBy(() -> query(innerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Access Denied.*");
        assertThatThrownBy(() -> query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.outer_view"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Access Denied.*");
        assertThatThrownBy(() -> query(testRoleSession, "SELECT * FROM memory.definer_views.outer_view"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Access Denied.*");

        // Grant it again and they all work again
        assertUpdate(getSession(), "GRANT SELECT ON TABLE memory.definer_views.base_table TO ROLE inner_view_owner WITH GRANT OPTION");
        assertThat(query(innerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view")).matches(tableContents);
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.outer_view")).matches(tableContents);
        assertThat(query(testRoleSession, "SELECT * FROM memory.definer_views.outer_view")).matches(tableContents);

        // Revoke the SELECT grant to outer_view_owner on the inner_view and verify that
        // inner_view_owner can still access the view, but outer_view_owner and test_role cannot
        assertUpdate(getSession(), "REVOKE SELECT ON TABLE memory.definer_views.inner_view FROM ROLE outer_view_owner");
        assertThat(query(innerViewOwnerSession, "SELECT * FROM memory.definer_views.inner_view")).matches(tableContents);
        assertThatThrownBy(() -> query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.outer_view"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Access Denied.*");
        assertThatThrownBy(() -> query(testRoleSession, "SELECT * FROM memory.definer_views.outer_view"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Access Denied.*");

        // Grant it again and they all work again
        assertUpdate(getSession(), "GRANT SELECT ON TABLE memory.definer_views.inner_view TO ROLE outer_view_owner WITH GRANT OPTION");
        assertThat(query(outerViewOwnerSession, "SELECT * FROM memory.definer_views.outer_view")).matches(tableContents);
        assertThat(query(testRoleSession, "SELECT * FROM memory.definer_views.outer_view")).matches(tableContents);

        // Clean up
        assertUpdate("DROP VIEW memory.definer_views.outer_view");
        assertUpdate("DROP VIEW memory.definer_views.inner_view");
        assertUpdate("DROP TABLE memory.definer_views.base_table");
        assertUpdate("DROP SCHEMA memory.definer_views");
        assertUpdate("DROP ROLE test_role");
        assertUpdate("DROP ROLE outer_view_owner");
        assertUpdate("DROP ROLE inner_view_owner");
        assertUpdate("DROP ROLE base_table_owner");
    }

    @Test
    public void testRowFilterOwnership()
    {
        MaterializedResult baseTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("filtered", true)
                .row("unfiltered", false)
                .build();
        MaterializedResult filteredTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("filtered", true)
                .build();

        assertUpdate("CREATE SCHEMA memory.row_filters");

        // filterOwner gets CREATE_TABLE on the schema
        Session filterOwner = createUserAndGrantedRole("filter_owner");
        RoleId filterOwnerRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("filter_owner"));

        assertUpdate("GRANT CREATE ON schema memory.row_filters TO ROLE filter_owner");

        // Create the base table, insert a row and verify the contents
        assertUpdate(filterOwner, "CREATE TABLE memory.row_filters.base_table(base_col1 VARCHAR, base_col2 BOOLEAN)");
        assertUpdate(filterOwner, "INSERT INTO memory.row_filters.base_table VALUES('filtered', true), ('unfiltered', false)", 2);
        assertThat(query(filterOwner, "SELECT * FROM memory.row_filters.base_table")).matches(baseTableContents);

        // Create the subquery table, insert a row and verify the contents
        assertUpdate(filterOwner, "CREATE TABLE memory.row_filters.subquery_table(match_column VARCHAR)");
        assertUpdate(filterOwner, "INSERT INTO memory.row_filters.subquery_table VALUES('filtered')", 1);
        MaterializedResult subqueryTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row("filtered")
                .build();
        TestingAccountClient client = getTestingAccountClient();
        assertThat(query(filterOwner, "SELECT * FROM memory.row_filters.subquery_table")).matches(subqueryTableContents);

        // Make the row filter, owned by filterOwner
        RowFilterId rowFilterId = client.createRowFilter(
                "base_filter",
                "EXISTS (SELECT 1 FROM memory.row_filters.subquery_table WHERE match_column = base_col1)",
                filterOwnerRoleId);

        // Make the policy for the filter
        createUserAndGrantedRole("policy_enabler");
        RoleId policyEnablerId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("policy_enabler"));

        TagId tagId = client.createTag("tag1");
        PolicyId policyId = client.createRowFilterPolicy("row_filter_policy", "memory.row_filters.base_table", "has_tag(tag1)", policyEnablerId, rowFilterId);

        // Apply the tag to the base_table
        client.applyTag(tagId, EntityKind.TABLE, "memory.row_filters.base_table");

        // Show that the row filter is in effect for filterOwner
        assertUpdate("GRANT policy_enabler TO ROLE filter_owner");
        assertThat(query(filterOwner, "SELECT * FROM memory.row_filters.base_table")).matches(filteredTableContents);

        // Create a "reader" who doesn't have access to subquery_table
        Session tableReaderSession = createUserAndGrantedRole("table_reader");
        assertUpdate("GRANT SELECT ON table memory.row_filters.base_table TO ROLE table_reader");
        assertUpdate("GRANT policy_enabler TO ROLE table_reader");

        // Show that directly accessing the subquery_table fails
        assertThatThrownBy(() -> query(tableReaderSession, "SELECT * FROM memory.row_filters.subquery_table"))
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Access Denied: Cannot select from columns [match_column] in table or view memory.row_filters.subquery_table: Relation not found or not allowed");

        // Show that we only get the filtered row when querying the base_table
        assertThat(query(tableReaderSession, "SELECT * FROM memory.row_filters.base_table")).matches(filteredTableContents);

        // Clean up
        client.deletePolicy(policyId);
        client.deleteRowFilter(rowFilterId);
        assertUpdate("DROP TABLE memory.row_filters.subquery_table");
        assertUpdate("DROP TABLE memory.row_filters.base_table");
        client.deleteTag(tagId);
        assertUpdate("DROP ROLE policy_enabler");
        assertUpdate("DROP ROLE table_reader");
        assertUpdate("DROP ROLE filter_owner");
        assertUpdate("DROP schema row_filters");
    }

    @Test
    public void testColumnMasks()
    {
        String maskedColumnValue = "I_AM_MASKED";
        MaterializedResult unmaskedTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("unmasked_1", true)
                .row("unmasked_2", false)
                .build();
        MaterializedResult maskedTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row(maskedColumnValue, true)
                .row(maskedColumnValue, false)
                .build();

        assertUpdate("CREATE SCHEMA memory.column_masks");

        // maskOwner gets CREATE_TABLE on the schema
        Session maskOwner = createUserAndGrantedRole("mask_owner");
        RoleId maskOwnerRoleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("mask_owner"));

        assertUpdate("GRANT CREATE ON schema memory.column_masks TO ROLE mask_owner");

        // Create the base table, insert a row and verify the contents
        assertUpdate(maskOwner, "CREATE TABLE memory.column_masks.column_mask_test_table(base_col1 VARCHAR, base_col2 BOOLEAN)");
        assertUpdate(maskOwner, "INSERT INTO memory.column_masks.column_mask_test_table VALUES('unmasked_1', true), ('unmasked_2', false)", 2);
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(unmaskedTableContents);

        TestingAccountClient client = getTestingAccountClient();

        // Make the column mask, owned by maskOwner
        ColumnMaskId columnMaskId = client.createColumnMask(
                "base_mask",
                "'I_AM_MASKED'",
                ColumnMaskType.VARCHAR,
                maskOwnerRoleId);

        // Make the policy for the mask
        createUserAndGrantedRole("policy_enabler");
        RoleId policyEnablerId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession()))
                .get(new RoleName("policy_enabler"));

        TagId tagId = client.createTag("tag1");
        PolicyId policyId = client.createColumnMaskPolicy("column_mask_policy", "memory.column_masks.column_mask_test_table", "has_tag(tag1)", policyEnablerId, columnMaskId);

        // Show that before the tag is applied, the mask owner sees unmasked results
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(unmaskedTableContents);

        // Apply the tag to base_col1 of the column_mask_test_table
        client.applyTag(tagId, EntityKind.COLUMN, "memory.column_masks.column_mask_test_table.base_col1");

        // Show that before the enabling role is granted, the mask_owner sees unmasked results
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(unmaskedTableContents);

        // Grant the enabling role to mask_owner
        assertUpdate("GRANT policy_enabler TO ROLE mask_owner");

        // Show that mask_owner sees masked results
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(maskedTableContents);

        // Create an unrelated "reader" role, and grant SELECT on the table
        Session tableReader = createUserAndGrantedRole("table_reader");
        assertUpdate("GRANT SELECT ON table memory.column_masks.column_mask_test_table TO ROLE table_reader");

        // Show that before being granted , the table_reader sees unmasked results
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(unmaskedTableContents);

        // Grant the enabling role to table_reader
        assertUpdate("GRANT policy_enabler TO ROLE table_reader");

        // Show that table_reader now sees masked results
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(maskedTableContents);

        // Update the column mask expression
        client.updateColumnMask(columnMaskId, "UPDATED_MASK", "'UPDATED_MASK'", ColumnMaskType.VARCHAR);
        MaterializedResult updatedMaskedTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("UPDATED_MASK", true)
                .row("UPDATED_MASK", false)
                .build();

        // Show that mask_owner and table_reader see new results
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(updatedMaskedTableContents);
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(updatedMaskedTableContents);

        // Delete that policy and mask
        client.deletePolicy(policyId);
        client.deleteColumnMask(columnMaskId);

        // Show that mask_owner and table_reader see unmasked results
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(unmaskedTableContents);
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(unmaskedTableContents);

        // Create a policy and mask that depends on a table that only mask_owner can access
        assertUpdate(maskOwner, "CREATE TABLE memory.column_masks.subquery_table(match_column VARCHAR)");
        assertUpdate(maskOwner, "INSERT INTO memory.column_masks.subquery_table VALUES('_match_proof')", 1);
        MaterializedResult subqueryTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row("_match_proof")
                .build();

        // mask_owner can query subquery_table
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.subquery_table")).matches(subqueryTableContents);

        // table_reader cannot query subquery_table
        assertThatThrownBy(() -> query(tableReader, "SELECT * FROM memory.column_masks.subquery_table"))
                .isInstanceOf(QueryFailedException.class)
                .hasMessage("Access Denied: Cannot select from columns [match_column] in table or view memory.column_masks.subquery_table: Relation not found or not allowed");

        // The column mask depends on subquery_table
        columnMaskId = client.createColumnMask(
                "new_mask",
                "concat(base_col1, (SELECT match_column FROM memory.column_masks.subquery_table))",
                ColumnMaskType.VARCHAR,
                maskOwnerRoleId);

        // Make the new policy
        policyId = client.createColumnMaskPolicy("column_mask_policy", "memory.column_masks.column_mask_test_table", "has_tag(tag1)", policyEnablerId, columnMaskId);

        maskedTableContents = MaterializedResult.resultBuilder(getSession(), VARCHAR, BOOLEAN)
                .row("unmasked_1_match_proof", true)
                .row("unmasked_2_match_proof", false)
                .build();

        // Both mask_owner and table_reader see the masked values when querying column_mask_test_table
        assertThat(query(maskOwner, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(maskedTableContents);
        assertThat(query(tableReader, "SELECT * FROM memory.column_masks.column_mask_test_table")).matches(maskedTableContents);

        // Clean up
        client.deletePolicy(policyId);
        client.deleteColumnMask(columnMaskId);
        assertUpdate("DROP TABLE memory.column_masks.column_mask_test_table");
        assertUpdate("DROP TABLE memory.column_masks.subquery_table");
        client.deleteTag(tagId);
        assertUpdate("DROP ROLE policy_enabler");
        assertUpdate("DROP ROLE table_reader");
        assertUpdate("DROP ROLE mask_owner");
        assertUpdate("DROP schema column_masks");
    }

    @Test
    public void testSystemBuiltinTableFunctions()
    {
        // Test the functions with and without the system.builtin. prefix
        for (String catalogAndSchema : ImmutableList.of("", "system.builtin.")) {
            // Test exclude_columns
            assertThat(query("""
                SELECT *
                FROM TABLE(%sexclude_columns(
                                    input => TABLE(tpch.tiny.region),
                                    columns => DESCRIPTOR(regionkey, comment)))
                """.formatted(catalogAndSchema)))
                    .skippingTypesCheck()
                    .matches("VALUES ('AFRICA'), ('AMERICA'), ('ASIA'), ('EUROPE'), ('MIDDLE EAST')");

            // Test sequence
            assertThat(query("""
                SELECT *
                FROM TABLE(%ssequence(
                                start => 1,
                                stop => 3,
                                step => 1))
                """.formatted(catalogAndSchema)))
                    .skippingTypesCheck()
                    .matches("VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT)");
        }
    }

    @Test
    public void testLiveCatalogAddDrop()
    {
        CatalogVersion tpchCatalogVersion = testingGalaxyCatalogInfoSupplier.getOnlyVersionForCatalogName("tpch");
        CatalogVersion memeoryCatalogVersion = testingGalaxyCatalogInfoSupplier.getOnlyVersionForCatalogName("memory");
        assertThat(query(sessionFor(tpchCatalogVersion, memeoryCatalogVersion), "SHOW CATALOGS"))
                .matches("VALUES 'tpch', 'memory', 'system'");
        assertThat(query(sessionFor(tpchCatalogVersion), "SHOW CATALOGS"))
                .matches("VALUES 'tpch', 'system'");
        assertThat(query(sessionFor(memeoryCatalogVersion), "SHOW CATALOGS"))
                .matches("VALUES 'memory', 'system'");
        assertThat(query(sessionFor(), "SHOW CATALOGS"))
                .matches("VALUES 'system'");

        assertThat(query(sessionFor(tpchCatalogVersion), "SELECT * FROM tpch.tiny.nation")).succeeds();
        assertQueryFails(sessionFor(memeoryCatalogVersion), "SELECT * FROM tpch.tiny.nation", ".* Catalog 'tpch' does not exist");
    }

    @Test
    public void testLiveCatalogLifeCycle()
    {
        CatalogVersion tpchCatalogVersion = testingGalaxyCatalogInfoSupplier.getOnlyVersionForCatalogName("tpch");
        String catalogName = "catalog_" + randomNameSuffix();
        CatalogId catalogId = testingAccountClient.getOrCreateCatalog(catalogName);
        // create memory plugin without read only and write a table to it
        GalaxyCatalogArgs catalogV1 = new GalaxyCatalogArgs(
                testingAccountClient.getAccountId(),
                new CatalogVersion(catalogId, new Version(new Random().nextLong(0, Long.MAX_VALUE))));
        GalaxyCatalogInfo catalogInfoV1 = new GalaxyCatalogInfo(
                new CatalogProperties(toCatalogHandle(catalogName, catalogV1.catalogVersion()), new ConnectorName("memory"), ImmutableMap.of()),
                false);
        testingGalaxyCatalogInfoSupplier.addCatalog(catalogV1, catalogInfoV1);
        assertThat(query(sessionFor(tpchCatalogVersion, catalogV1.catalogVersion()),
                """
                        CREATE SCHEMA %s.test_schema
                        """.formatted(catalogName))).succeeds();
        assertThat(query(sessionFor(tpchCatalogVersion, catalogV1.catalogVersion()),
                """
                        CREATE TABLE %s.test_schema.testing_table AS
                        SELECT * FROM tpch.tiny.nation
                        """.formatted(catalogName))).succeeds();
        // Create and assert permissions on one version of the catalog
        assertUpdate(sessionFor(tpchCatalogVersion, catalogV1.catalogVersion()),
                """
                        REVOKE CREATE ON SCHEMA %s.test_schema FROM ROLE public
                        """.formatted(catalogName));
        assertQueryFails(sessionFor(publicSession(), tpchCatalogVersion, catalogV1.catalogVersion()), "CREATE TABLE %s.test_schema.public_table(dummy int)".formatted(catalogName), ".* Role public does not have the privilege CREATE_TABLE .*");

        // create new version with read only and assert write fails
        GalaxyCatalogArgs catalogV2 = new GalaxyCatalogArgs(
                testingAccountClient.getAccountId(),
                new CatalogVersion(catalogId, new Version(new Random().nextLong(0, Long.MAX_VALUE))));
        GalaxyCatalogInfo catalogInfoV2 = new GalaxyCatalogInfo(
                new CatalogProperties(toCatalogHandle(catalogName, catalogV1.catalogVersion()), new ConnectorName("memory"), ImmutableMap.of()),
                true);
        testingGalaxyCatalogInfoSupplier.addCatalog(catalogV2, catalogInfoV2);
        assertQueryFails(sessionFor(tpchCatalogVersion, catalogV2.catalogVersion()),
                "CREATE SCHEMA %s.test_schema".formatted(catalogName), ".* Catalog " + catalogName + " only allows read-only access");
        // smoke test assert privileges haven't changed across versions
        assertQueryFails(sessionFor(publicSession(), tpchCatalogVersion, catalogV1.catalogVersion()), "CREATE TABLE %s.test_schema.public_table(dummy int)".formatted(catalogName), ".* Role public does not have the privilege CREATE_TABLE .*");
    }

    @Test
    public void testLiveCatalogGC()
    {
        try {
            LiveCatalogsTransactionManager liveCatalogsTransactionManager = (LiveCatalogsTransactionManager) getQueryRunner().getTransactionManager();
            TestingClock testingClock = (TestingClock) getDistributedQueryRunner().getCoordinator().getInstance(Key.get(Clock.class, ForTransactionManager.class));
            String catalogName = "catalog_" + randomNameSuffix();
            CatalogId catalogId = testingAccountClient.getOrCreateCatalog(catalogName);
            // create memory plugin without read only and write a table to it
            GalaxyCatalogArgs catalogV1 = new GalaxyCatalogArgs(testingAccountClient.getAccountId(), new CatalogVersion(catalogId, new Version(1)));
            GalaxyCatalogArgs catalogV2 = new GalaxyCatalogArgs(testingAccountClient.getAccountId(), new CatalogVersion(catalogId, new Version(2)));
            GalaxyCatalogInfo catalogInfoV1 = new GalaxyCatalogInfo(
                    new CatalogProperties(
                            toCatalogHandle(catalogName, catalogV1.catalogVersion()),
                            new ConnectorName("memory"),
                            ImmutableMap.of()),
                    false);
            testingGalaxyCatalogInfoSupplier.addCatalog(catalogV1, catalogInfoV1);
            testingGalaxyCatalogInfoSupplier.addCatalog(catalogV2, catalogInfoV1);
            // Load in both catalogs
            assertThat(query(sessionFor(catalogV1.catalogVersion()), "SELECT 1")).succeeds();
            assertThat(query(sessionFor(catalogV2.catalogVersion()), "SELECT 1")).succeeds();
            assertThat(liveCatalogsTransactionManager.contains(catalogV1)).isTrue();
            assertThat(liveCatalogsTransactionManager.contains(catalogV2)).isTrue();
            // move clock past old version retention and clean up older version
            testingClock.increment(liveCatalogsTransactionManager.getMaxOldVersionStaleness().toMillis() + 1, TimeUnit.MILLISECONDS);
            liveCatalogsTransactionManager.cleanUpStaleCatalogs();
            assertThat(liveCatalogsTransactionManager.contains(catalogV1)).isFalse();
            assertThat(liveCatalogsTransactionManager.contains(catalogV2)).isTrue();
            // move clock past all catalog retention and clean up both catalogs
            testingClock.increment(liveCatalogsTransactionManager.getMaxCatalogStaleness().toMillis() - liveCatalogsTransactionManager.getMaxOldVersionStaleness().toMillis() + 1, TimeUnit.MILLISECONDS);
            liveCatalogsTransactionManager.cleanUpStaleCatalogs();
            assertThat(liveCatalogsTransactionManager.contains(catalogV1)).isFalse();
            assertThat(liveCatalogsTransactionManager.contains(catalogV2)).isFalse();
        } finally {
            // memory catalog is a casualty in the test. Recreate schema
            initWithQueryRunner(getQueryRunner());
        }
    }

    private Session sessionFor(CatalogVersion... versions)
    {
        return sessionFor(getSession(), versions);
    }

    private Session sessionFor(Session session, CatalogVersion... versions)
    {
        return Session.builder(session)
                .setQueryCatalogs(Arrays.stream(versions).toList())
                .build();
    }

    private MaterializedResult varcharColumnResult(String... values)
    {
        return varcharColumnResult(ImmutableSet.copyOf(values));
    }

    private MaterializedResult varcharColumnResult(Set<String> values)
    {
        MaterializedResult.Builder resultBuilder = resultBuilder(getSession(), VARCHAR);
        values.forEach(resultBuilder::row);
        return resultBuilder.build();
    }

    private Session publicSession()
    {
        TestingAccountClient accountClient = getTestingAccountClient();
        RoleId publicRoleId = accountClient.getPublicRoleId();

        Identity identity = createIdentity(
                accountClient.getAdminEmail(),
                accountClient.getAccountId(),
                accountClient.getAdminUserId(),
                publicRoleId,
                accountClient.getAdminTrinoAccessToken(),
                PORTAL);

        return Session.builder(getSession())
                .setIdentity(identity)
                .build();
    }

    private Session createUserAndGrantedRole(String roleName)
    {
        assertUpdate("CREATE ROLE " + roleName);
        Session session = newUserSession(roleName);
        assertUpdate("GRANT %s TO USER \"%s\"".formatted(roleName, session.getUser()));
        return session;
    }

    private Session newUserSession(String roleName)
    {
        RoleId roleId = getTrinoSecurityApi().listRoles(toDispatchSession(getSession())).get(new RoleName(roleName));
        verifyNotNull(roleId, "roleId for roleName %s is null", roleName);
        return newUserSession(roleId);
    }

    private Session newUserSession(RoleId roleId)
    {
        TestingAccountClient accountClient = getTestingAccountClient();
        TestUser user = accountClient.createUser();

        Identity identity = createIdentity(
                user.getEmail(),
                accountClient.getAccountId(),
                user.getUserId(),
                roleId,
                accountClient.getAdminTrinoAccessToken(),
                PORTAL);

        return Session.builder(getSession())
                .setIdentity(identity)
                .build();
    }

    private TestingAccountClient getTestingAccountClient()
    {
        TestingTrinoServer coordinator = this.getDistributedQueryRunner().getCoordinator();
        TestingAccountClient accountClient = coordinator.getInstance(Key.get(TestingAccountClient.class));
        return accountClient;
    }

    private TrinoSecurityApi getTrinoSecurityApi()
    {
        TestingTrinoServer coordinator = this.getDistributedQueryRunner().getCoordinator();
        return coordinator.getInstance(Key.get(TrinoSecurityApi.class));
    }
}
