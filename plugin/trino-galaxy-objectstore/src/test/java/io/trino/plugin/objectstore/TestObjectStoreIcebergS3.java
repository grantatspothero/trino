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

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestObjectStoreIcebergS3
        extends BaseObjectStoreS3Test
{
    @Parameters("s3.bucket")
    public TestObjectStoreIcebergS3(String bucketName)
    {
        super(ICEBERG, "partitioning", bucketName);
    }

    @Override
    protected void validateDataFiles(String partitionColumn, String tableName, String location)
    {
        getActiveFiles(tableName).forEach(dataFile ->
        {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            String partitionPart = partitionColumn.isEmpty() ? "" : partitionColumn + "=[a-z0-9]+/";
            assertThat(dataFile).matches("^" + locationDirectory + "data/" + partitionPart + "[a-zA-Z0-9_-]+.orc$");
            verifyPathExist(dataFile);
        });
    }

    @Override
    protected void validateMetadataFiles(String location)
    {
        getAllMetadataDataFilesFromTableDirectory(location).forEach(metadataFile ->
        {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            assertThat(metadataFile).matches("^" + locationDirectory + "metadata/[a-zA-Z0-9_-]+.(avro|metadata.json|stats)$");
            verifyPathExist(metadataFile);
        });
    }

    @Override
    protected void validateTableLocation(String tableName, String location)
    {
        if (location.endsWith("/")) {
            //Iceberg removes trailing slash from location, and it's expected.
            assertThat(getTableLocation(tableName) + "/").isEqualTo(location);
        }
        else {
            assertThat(getTableLocation(tableName)).isEqualTo(location);
        }
    }

    private Set<String> getAllMetadataDataFilesFromTableDirectory(String tableLocation)
    {
        return getTableFiles(tableLocation).stream()
                .filter(path -> path.contains("/metadata"))
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    protected Set<String> getAllDataFilesFromTableDirectory(String tableLocation)
    {
        return getTableFiles(tableLocation).stream()
                .filter(path -> path.contains("/data"))
                .collect(Collectors.toUnmodifiableSet());
    }

    @Test(dataProvider = "locationPatternsDataProvider")
    public void testAnalyzeWithProvidedTableLocation(boolean partitioned, String locationPattern)
    {
        String tableName = "test_analyze_" + randomNameSuffix();
        String location = locationPattern.formatted(bucketName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioning = ARRAY['col_str']" : "");

        assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (location = '" + location + "'" + partitionQueryPart + ") " +
                "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);

        assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

        String expectedStatistics = """
                VALUES
                ('col_str', null, 4.0, 0.0, null, null, null),
                ('col_int', null, 4.0, 0.0, null, 1, 4),
                (null, null, null, null, 4.0, null, null)""";

        //Check extended statistics collection on write
        assertQuery("SHOW STATS FOR " + tableName, expectedStatistics);

        // drop stats
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS");
        //Check extended statistics collection explicitly
        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, expectedStatistics);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithIncorrectLocation()
    {
        String tableName = "test_create_table_with_incorrect_location_" + randomNameSuffix();
        String location = "s3://%s/galaxy/a#hash/%s".formatted(bucketName, tableName);

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar) WITH (location = '" + location + "')"))
                .hasMessageContaining("Fragment is not allowed in a file system location");
    }

    @Test
    public void testCTASWithIncorrectLocation()
    {
        String tableName = "test_create_table_with_incorrect_location_" + randomNameSuffix();
        String location = "s3://%s/galaxy/a#hash/%s".formatted(bucketName, tableName);

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName +
                " WITH (location = '" + location + "')" +
                " AS SELECT * FROM tpch.tiny.nation"))
                .hasMessageContaining("Fragment is not allowed in a file system location");
    }

    @Test
    public void testCreateSchemaWithIncorrectLocation()
    {
        String schemaName = "test_create_schema_with_incorrect_location_" + randomNameSuffix();
        String schemaLocation = "s3://%s/galaxy/a#hash/%s".formatted(bucketName, schemaName);
        String tableName = "test_basic_operations_table_" + randomNameSuffix();
        String qualifiedTableName = schemaName + "." + tableName;

        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaLocation + "')");
        assertThat(getSchemaLocation(schemaName)).isEqualTo(schemaLocation);

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + qualifiedTableName + "(col_str varchar, col_int int)"))
                .hasMessageContaining("Fragment is not allowed in a file system location");

        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + qualifiedTableName + " AS SELECT * FROM tpch.tiny.nation"))
                .hasMessageContaining("Fragment is not allowed in a file system location");

        assertUpdate("DROP SCHEMA " + schemaName);
    }
}
