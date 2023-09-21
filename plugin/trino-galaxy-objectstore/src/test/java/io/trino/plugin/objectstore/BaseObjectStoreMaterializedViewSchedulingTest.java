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
package io.trino.plugin.objectstore;

import io.starburst.stargate.accountwork.client.TestingMaterializedViewSchedulerService;
import io.trino.Session;
import io.trino.server.security.galaxy.GalaxyTestHelper;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// The testing materialized view scheduling service is not thread safe.
@Execution(SAME_THREAD)
public abstract class BaseObjectStoreMaterializedViewSchedulingTest
        extends AbstractTestQueryFramework
{
    protected static final String TEST_CATALOG = "iceberg";
    protected static final String TEST_CATALOG_WITHOUT_SCHEDULING = "iceberg_no_scheduling";

    protected final String schemaName = "test_materialized_view_scheduling_" + randomNameSuffix();
    protected GalaxyTestHelper galaxyTestHelper;
    protected TestingMaterializedViewSchedulerService.WorkManager accountWorkManager;
    protected TestingMaterializedViewSchedulerService accountWorkService;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        galaxyTestHelper = closeAfterClass(new GalaxyTestHelper());
        galaxyTestHelper.initialize();

        accountWorkManager = new TestingMaterializedViewSchedulerService.WorkManager(sql -> this.getQueryRunner().execute(sql));
        accountWorkService = closeAfterClass(new TestingMaterializedViewSchedulerService(accountWorkManager));

        DistributedQueryRunner queryRunner = setupQueryRunner();
        queryRunner.execute("GRANT SELECT ON tpch.\"*\".\"*\" TO ROLE %s WITH GRANT OPTION".formatted(ACCOUNT_ADMIN));
        queryRunner.execute(createSchemaSql(TEST_CATALOG, schemaName));
        queryRunner.execute("GRANT ALL PRIVILEGES ON SCHEMA %s.%s TO ROLE %s WITH GRANT OPTION".formatted(TEST_CATALOG, schemaName, ACCOUNT_ADMIN));
        queryRunner.execute("GRANT ALL PRIVILEGES ON SCHEMA %s.%s TO ROLE %s WITH GRANT OPTION".formatted(TEST_CATALOG_WITHOUT_SCHEDULING, schemaName, ACCOUNT_ADMIN));

        return queryRunner;
    }

    protected abstract DistributedQueryRunner setupQueryRunner()
            throws Exception;

    protected abstract String createSchemaSql(String catalogName, String schemaName);

    @AfterAll
    public void tearDown()
    {
        getQueryRunner().execute("DROP SCHEMA %s.%s".formatted(TEST_CATALOG, schemaName));
    }

    @Test
    public void testScheduledRefreshDisabled()
    {
        String materializedViewName = "test_scheduled_refresh_disabled" + randomNameSuffix();
        String schema = "without_scheduling_" + randomNameSuffix();
        Session withSchedulingDisabled = Session.builder(getSession())
                .setCatalog(TEST_CATALOG_WITHOUT_SCHEDULING)
                .setSchema(schema)
                .build();
        assertUpdate(createSchemaSql(TEST_CATALOG_WITHOUT_SCHEDULING, schema));
        try {
            assertThatThrownBy(() -> query(withSchedulingDisabled, "CREATE MATERIALIZED VIEW " + materializedViewName + " WITH (refresh_schedule = '0 0 * * *') AS SELECT 1 AS c"))
                    .hasMessageContaining("materialized view property 'refresh_schedule' does not exist");
            assertQuery("SELECT catalog_name FROM system.metadata.materialized_view_properties WHERE property_name = 'refresh_schedule'", "VALUES '" + TEST_CATALOG + "'");
        }
        finally {
            assertUpdate("DROP SCHEMA " + schema);
        }
    }

    @Test
    public void testScheduledRefresh()
    {
        String materializedViewName = "test_scheduled_refresh" + randomNameSuffix();
        String tableName = "test_materialized_table_" + randomNameSuffix();
        CatalogSchemaTableName catalogMaterializedViewName = new CatalogSchemaTableName(TEST_CATALOG, new SchemaTableName(schemaName, materializedViewName));

        assertUpdate("CREATE TABLE " + tableName + " (a varchar)");
        computeActual("CREATE MATERIALIZED VIEW " + materializedViewName + " WITH (refresh_schedule = '0 0 * * *') AS SELECT * FROM " + tableName);

        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES '42'", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES '45'", 1);

            assertQuery("SHOW CREATE MATERIALIZED VIEW " + materializedViewName, "VALUES '" +
                    "CREATE MATERIALIZED VIEW " + catalogMaterializedViewName + "\n" +
                    "WITH (\n" +
                    "   refresh_schedule = ''0 0 * * *'',\n" +
                    "   storage_schema = ''" + schemaName + "''\n" +
                    ") AS\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  " + tableName + "'");
            accountWorkManager.runScheduledRefreshesForSchedule("0 0 * * *");
            assertThat(getExplainPlan("SELECT * FROM " + materializedViewName, ExplainType.Type.IO)).doesNotContain(tableName);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
        }
    }

    @Test
    public void testExternallyDroppedScheduledRefresh()
    {
        String materializedViewName = "test_dropped_scheduled_refresh" + randomNameSuffix();
        String tableName = "test_materialized_table_" + randomNameSuffix();
        CatalogSchemaTableName catalogMaterializedViewName = new CatalogSchemaTableName(TEST_CATALOG, new SchemaTableName(schemaName, materializedViewName));

        assertUpdate("CREATE TABLE " + tableName + " (a varchar)");
        computeActual("CREATE MATERIALIZED VIEW " + materializedViewName + " WITH (refresh_schedule = '0 0 * * *') AS SELECT * FROM " + tableName);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES '42'", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES '45'", 1);
            accountWorkManager.runScheduledRefreshesForSchedule("0 0 * * *");

            accountWorkManager.deleteScheduledRefresh(accountWorkManager.scheduleIdForMaterializedView(
                    catalogMaterializedViewName.getCatalogName(),
                    catalogMaterializedViewName.getSchemaTableName().getSchemaName(),
                    catalogMaterializedViewName.getSchemaTableName().getTableName()).orElseThrow());
            assertQuery("SHOW CREATE MATERIALIZED VIEW " + materializedViewName, "VALUES '" +
                    "CREATE MATERIALIZED VIEW " + catalogMaterializedViewName + "\n" +
                    "WITH (\n" +
                    "   storage_schema = ''" + schemaName + "''\n" +
                    ") AS\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  " + tableName + "'");
            assertThat(getExplainPlan("SELECT * FROM " + materializedViewName, ExplainType.Type.IO)).doesNotContain(tableName);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
        }
    }

    @Test
    public void testAlterScheduledRefresh()
    {
        String materializedViewName = "test_alter_scheduled_refresh" + randomNameSuffix();
        String tableName = "test_materialized_table_" + randomNameSuffix();
        CatalogSchemaTableName catalogMaterializedViewName = new CatalogSchemaTableName(TEST_CATALOG, new SchemaTableName(schemaName, materializedViewName));

        assertUpdate("CREATE TABLE " + tableName + " (a varchar)");
        assertUpdate("CREATE MATERIALIZED VIEW " + materializedViewName + " WITH (refresh_schedule = '0 0 * * *') AS SELECT * FROM " + tableName);
        try {
            assertUpdate("ALTER MATERIALIZED VIEW " + materializedViewName + " SET PROPERTIES refresh_schedule = '1 1 * * *'");

            accountWorkManager.runScheduledRefreshesForSchedule("0 0 * * *");
            assertThat(getExplainPlan("SELECT * FROM " + materializedViewName, ExplainType.Type.IO)).contains(tableName);

            accountWorkManager.runScheduledRefreshesForSchedule("1 1 * * *");
            assertThat(getExplainPlan("SELECT * FROM " + materializedViewName, ExplainType.Type.IO)).doesNotContain(tableName);

            assertQuery("SHOW CREATE MATERIALIZED VIEW " + materializedViewName, "VALUES '" +
                    "CREATE MATERIALIZED VIEW " + catalogMaterializedViewName + "\n" +
                    "WITH (\n" +
                    "   refresh_schedule = ''1 1 * * *'',\n" +
                    "   storage_schema = ''" + schemaName + "''\n" +
                    ") AS\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  " + tableName + "'");

            accountWorkManager.deleteScheduledRefresh(accountWorkManager.scheduleIdForMaterializedView(
                    catalogMaterializedViewName.getCatalogName(),
                    catalogMaterializedViewName.getSchemaTableName().getSchemaName(),
                    catalogMaterializedViewName.getSchemaTableName().getTableName()).orElseThrow());
            assertUpdate("ALTER MATERIALIZED VIEW " + materializedViewName + " SET PROPERTIES refresh_schedule = '2 2 * * *'");
            assertQuery("SHOW CREATE MATERIALIZED VIEW " + materializedViewName, "VALUES '" +
                    "CREATE MATERIALIZED VIEW " + catalogMaterializedViewName + "\n" +
                    "WITH (\n" +
                    "   refresh_schedule = ''2 2 * * *'',\n" +
                    "   storage_schema = ''" + schemaName + "''\n" +
                    ") AS\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  " + tableName + "'");

            assertUpdate("ALTER MATERIALIZED VIEW " + materializedViewName + " SET PROPERTIES refresh_schedule = DEFAULT");
            assertQuery("SHOW CREATE MATERIALIZED VIEW " + materializedViewName, "VALUES '" +
                    "CREATE MATERIALIZED VIEW " + catalogMaterializedViewName + "\n" +
                    "WITH (\n" +
                    "   storage_schema = ''" + schemaName + "''\n" +
                    ") AS\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  " + tableName + "'");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
        }
    }

    @Test
    public void testSetScheduleOnExistingMaterializedView()
    {
        String materializedViewName = "test_set_schedule_on_existing_mv_" + randomNameSuffix();
        String tableName = "test_materialized_table_" + randomNameSuffix();
        CatalogSchemaTableName catalogMaterializedViewName = new CatalogSchemaTableName(TEST_CATALOG, new SchemaTableName(schemaName, materializedViewName));

        assertUpdate("CREATE TABLE " + tableName + " (a varchar)");
        assertUpdate("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT * FROM " + tableName);
        try {
            assertUpdate("ALTER MATERIALIZED VIEW " + materializedViewName + " SET PROPERTIES refresh_schedule = '1 1 * * *'");
            assertQuery("SHOW CREATE MATERIALIZED VIEW " + materializedViewName, "VALUES '" +
                    "CREATE MATERIALIZED VIEW " + catalogMaterializedViewName + "\n" +
                    "WITH (\n" +
                    "   refresh_schedule = ''1 1 * * *'',\n" +
                    "   storage_schema = ''" + schemaName + "''\n" +
                    ") AS\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  " + tableName + "'");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
        }
    }

    @Test
    public void testDroppingMaterializedViewDeletesScheduledRefresh()
    {
        String materializedViewName = "test_dropping_mv_deletes_scheduled_refresh" + randomNameSuffix();
        String tableName = "test_materialized_table_" + randomNameSuffix();
        CatalogSchemaTableName catalogMaterializedViewName = new CatalogSchemaTableName(TEST_CATALOG, new SchemaTableName(schemaName, materializedViewName));

        assertUpdate("CREATE TABLE " + tableName + " (a varchar)");
        assertUpdate("CREATE MATERIALIZED VIEW " + materializedViewName + " WITH (refresh_schedule = '0 0 * * *') AS SELECT * FROM " + tableName);
        try {
            assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
            assertThat(accountWorkManager.scheduleIdForMaterializedView(
                    catalogMaterializedViewName.getCatalogName(),
                    catalogMaterializedViewName.getSchemaTableName().getSchemaName(),
                    catalogMaterializedViewName.getSchemaTableName().getTableName()))
                    .isEmpty();
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + materializedViewName);
        }
    }
}
