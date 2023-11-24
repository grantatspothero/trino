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
package io.trino.plugin.galaxy.catalog;

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionApi;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.IdType;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestGalaxyCatalogLanguageFunctions
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "test";
    private static final String SCHEMA_NAME = "functions";

    private static final String SHOW_FUNCTIONS_QUERY = "SHOW FUNCTIONS FROM %s.%s".formatted(CATALOG_NAME, SCHEMA_NAME);

    private static final Identity IDENTITY = Identity.from(Identity.ofUser("user"))
            .withExtraCredentials(ImmutableMap.of(
                    "accountId", IdType.ACCOUNT_ID.randomId(AccountId::new).toString(),
                    "userId", IdType.USER_ID.randomId(UserId::new).toString(),
                    "roleId", IdType.ROLE_ID.randomId(RoleId::new).toString(),
                    "GalaxyTokenCredential", "dummy"))
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ImmutableMap.Builder<String, String> extraProperties = ImmutableMap.builder();
        extraProperties.put("sql.path", CATALOG_NAME + "." + SCHEMA_NAME)
                .put("sql.default-function-catalog", CATALOG_NAME)
                .put("sql.default-function-schema", SCHEMA_NAME);
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(CATALOG_NAME)
                        .setSchema(SCHEMA_NAME)
                        .setIdentity(IDENTITY)
                        .build())
                .setExtraProperties(extraProperties.buildOrThrow())
                .build();

        queryRunner.installPlugin(new GalaxyCatalogPlugin(binder -> binder.bind(GalaxyLanguageFunctionApi.class).toInstance(new TestingGalaxyFunctionApi())));

        queryRunner.createCatalog(CATALOG_NAME, GalaxyCatalogConnectorFactory.CONNECTOR_NAME, ImmutableMap.of("galaxy.account-url", "http://foo.com"));

        return queryRunner;
    }

    @Test
    public void testCrud()
    {
        assertQueryReturnsEmptyResult(SHOW_FUNCTIONS_QUERY);

        assertUpdate("CREATE FUNCTION f1 (x double) RETURNS double COMMENT 't88' RETURN x * 8.8");
        assertFunctions("f1");

        assertQueryFails("CREATE FUNCTION f1 (x double) RETURNS double COMMENT 't88' RETURN x * 8.8", "line 1:1: Function already exists");

        assertUpdate("CREATE FUNCTION f2 (x double) RETURNS double COMMENT 't88' RETURN x * 8.8");
        assertFunctions("f1", "f2");

        assertUpdate("DROP FUNCTION f1 (x double)");
        assertFunctions("f2");

        assertUpdate("DROP FUNCTION f2 (x double)");
        assertQueryReturnsEmptyResult(SHOW_FUNCTIONS_QUERY);

        assertQueryFails("DROP FUNCTION boom(varchar)", "line 1:1: Function not found");
    }

    @Test
    public void testFunctionCall()
    {
        assertUpdate("CREATE FUNCTION foo (x integer) RETURNS bigint COMMENT 't42' RETURN x * 42");
        assertQuery("SELECT foo(99)", "SELECT 4158");
        assertQueryFails("SELECT foo(2.9)", ".*Unexpected parameters.*");

        assertUpdate("DROP FUNCTION foo (x integer)");
    }

    private void assertFunctions(String... functions)
    {
        assertThat(computeActual(SHOW_FUNCTIONS_QUERY).project("Function").getOnlyColumn()).containsAll(Arrays.stream(functions).toList());
    }
}
