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

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.BatchDeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.hdfs.TrinoFileSystemCache;
import io.trino.plugin.hive.aws.AwsApiCallStats;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.server.security.galaxy.GalaxyTestHelper;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseObjectStoreS3GlueTest
        extends AbstractTestQueryFramework
{
    private static final String TEST_CATALOG = "objectstore";

    private final String defaultWarehouseDirectoryName = "warehouse_" + randomNameSuffix();
    private final String schemaName;
    private final TableType tableType;
    private final String bucketName;

    protected BaseObjectStoreS3GlueTest(TableType tableType, String bucketName)
    {
        this.tableType = requireNonNull(tableType, "tableType is null");
        schemaName = "test_objectstore_s3_glue_" + tableType.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        GalaxyTestHelper galaxyTestHelper = closeAfterClass(new GalaxyTestHelper());
        galaxyTestHelper.initialize();

        closeAfterClass(TrinoFileSystemCache.INSTANCE::closeAll);
        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> true));

        Map<String, String> properties = createObjectStoreProperties(
                tableType,
                ImmutableMap.<String, String>builder()
                        .putAll(locationSecurityServer.getClientConfig())
                        .put("galaxy.catalog-id", "c-1234567890")
                        .buildOrThrow(),
                "glue",
                ImmutableMap.of("hive.metastore.glue.default-warehouse-dir", format("s3://%s/galaxy/%s", bucketName, defaultWarehouseDirectoryName)),
                ImmutableMap.of(),
                ImmutableMap.of());
        DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder(TEST_CATALOG, schemaName)
                .setAccountClient(galaxyTestHelper.getAccountClient())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new ObjectStorePlugin())
                .addCatalog(TEST_CATALOG, "galaxy_objectstore", properties)
                .build();
        queryRunner.execute("CREATE SCHEMA %s.%s".formatted(TEST_CATALOG, schemaName));
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        cleanUpSchema(schemaName);
    }

    private static void cleanUpSchema(String schema)
    {
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        Set<String> tableNames = getPaginatedResults(
                glueClient::getTables,
                new GetTablesRequest().withDatabaseName(schema),
                GetTablesRequest::setNextToken,
                GetTablesResult::getNextToken,
                new AwsApiCallStats())
                .map(GetTablesResult::getTableList)
                .flatMap(Collection::stream)
                .map(Table::getName)
                .collect(toImmutableSet());
        glueClient.batchDeleteTable(new BatchDeleteTableRequest()
                .withDatabaseName(schema)
                .withTablesToDelete(tableNames));
        glueClient.deleteDatabase(new DeleteDatabaseRequest()
                .withName(schema));
    }

    @Test
    public void testSchemaNameEscape()
    {
        String schemaNameSuffix = randomNameSuffix();
        String schemaName = "../test_create_schema_escaped_" + schemaNameSuffix;
        String tableName = "test_table_schema_escaped_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA \"" + schemaName + "\"");
        // On S3, when creating the schema, there is no directory corresponding to the schema name created
        assertUpdate("CREATE TABLE \"" + schemaName + "\"." + tableName + " (col) AS VALUES 1", 1);

        assertQuery("SELECT * FROM \"" + schemaName + "\"." + tableName, "VALUES 1");
        String tableLocation = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM \"" + schemaName + "\"." + tableName);
        String schemaLocation = getSchemaLocation(schemaName);

        assertThat(schemaLocation).isEqualTo("s3://" + bucketName + "/galaxy/" + defaultWarehouseDirectoryName + "/..%2Ftest_create_schema_escaped_" + schemaNameSuffix);
        assertThat(tableLocation).startsWith("s3://" + bucketName + "/galaxy/" + defaultWarehouseDirectoryName + "/..%2Ftest_create_schema_escaped_" + schemaNameSuffix + "/" + tableName);

        assertUpdate("DROP TABLE \"" + schemaName + "\"." + tableName);
        assertUpdate("DROP SCHEMA \"" + schemaName + "\"");
    }

    @Test
    public void testDotsSchemaNameEscape()
    {
        assertThatThrownBy(() -> assertUpdate("CREATE SCHEMA \"..\""))
                .hasMessage("Invalid schema name");
    }

    @Test
    public void testTableNameEscape()
    {
        String tableNameSuffix = randomNameSuffix();
        String sourceTableName = "../test_create_table_name_escape_" + tableNameSuffix;
        String destinationTableName = "../test_ctas_table_name_escape_" + tableNameSuffix;

        assertUpdate("CREATE TABLE \"" + sourceTableName + "\" (c integer)");
        try {
            assertUpdate("INSERT INTO \"" + sourceTableName + "\" VALUES 1", 1);
            assertQuery("SELECT * FROM \"" + sourceTableName + "\"", "VALUES 1");

            assertUpdate("CREATE TABLE \"" + destinationTableName + "\" AS SELECT * FROM \"" + sourceTableName + "\"", 1);
            assertQuery("SELECT * FROM \"" + destinationTableName + "\"", "VALUES 1");

            Location schemaLocation = Location.of(getSchemaLocation(schemaName));
            String sourceTableLocation = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM \"" + sourceTableName + "\"");
            assertThat(sourceTableLocation).startsWith(schemaLocation.appendPath("..%2Ftest_create_table_name_escape_" + tableNameSuffix).toString());
            String destinationTableLocation = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM \"" + destinationTableName + "\"");
            assertThat(destinationTableLocation).startsWith(schemaLocation.appendPath("..%2Ftest_ctas_table_name_escape_" + tableNameSuffix).toString());
        }
        finally {
            assertUpdate("DROP TABLE \"" + sourceTableName + "\"");
            assertUpdate("DROP TABLE IF EXISTS \"" + destinationTableName + "\"");
        }
    }

    @Test
    public void testDotsTableNameEscape()
    {
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE \"..\" (c integer)"))
                .hasMessage("Invalid table name");
    }

    private String getSchemaLocation(String schemaName)
    {
        return findLocationInQuery("SHOW CREATE SCHEMA \"" + schemaName + "\"");
    }

    private String findLocationInQuery(String query)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual(query).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in" + query + " result");
    }
}
