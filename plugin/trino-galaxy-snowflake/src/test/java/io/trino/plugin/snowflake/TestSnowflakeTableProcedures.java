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
package io.trino.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugins.snowflake.SnowflakeServer;
import io.trino.plugin.objectstore.TestingLocationSecurityServer;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.TEST_WAREHOUSE;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestSnowflakeTableProcedures
        extends AbstractTestQueryFramework
{
    // TODO: Use objectstore iceberg connector to create/refresh the table from scratch. This scenario is tested in Galaxy integration-tests
    private static final String TABLE_LOCATION = requireNonNull(System.getProperty("snowflake.test.s3.register.table.location"), "snowflake.test.s3.register.table.location is not set");
    // Storage integration needs accountadmin privileges in Snowflake. So this has been pre-created using the role `arn:aws:iam::662260526581:role/galaxy_snowflake_storage_integration` in account `662260526581`.
    // The storage integration has access to the bucket `snowflake.test.s3.register.table.location` for `snowflake.test.server.role`
    private static final String STORAGE_INTEGRATION = requireNonNull(System.getProperty("snowflake.test.s3.storage.integration"), "snowflake.test.s3.storage.integration is not set");
    protected final SnowflakeServer server = new SnowflakeServer();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());
        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> galaxyCockroachContainer));
        return GalaxySnowflakeQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.of("snowflake.external-table-procedures.enabled", "true"))
                .withServer(server)
                .withLocationSecurityServer(new TestingLocationSecurityServer((session, location) -> !location.contains("denied")))
                .withAccountClient(testingAccountFactory.createAccountClient())
                .withWarehouse(Optional.of(TEST_WAREHOUSE))
                .withSchema(TEST_SCHEMA)
                .build();
    }

    @Test
    public void testRegisterTableProcedure()
    {
        String tableName = "test_register_table_" + randomNameSuffix();

        assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");

        assertUpdate(format("CALL system.register_table (CURRENT_SCHEMA, '%s', '%s', '%s')", tableName, TABLE_LOCATION, STORAGE_INTEGRATION));
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 10");

        assertUpdate(format("CALL system.unregister_table (CURRENT_SCHEMA, '%s')", tableName));
        assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");
    }

    @Test
    public void testRegisterTableProcedureLocationAccess()
    {
        String tableName = "test_register_table_" + randomNameSuffix();

        assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");
        assertQueryFails(format("CALL system.register_table (CURRENT_SCHEMA, '%s', '%s', '%s')", tableName, "s3://test-bucket/denied", STORAGE_INTEGRATION),
                "Access Denied: Role accountadmin is not allowed to use location: s3://test-bucket/denied");
    }
}
