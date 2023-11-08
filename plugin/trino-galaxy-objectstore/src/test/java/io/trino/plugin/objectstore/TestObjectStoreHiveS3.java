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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestObjectStoreHiveS3
        extends BaseObjectStoreS3Test
{
    public TestObjectStoreHiveS3()
    {
        super(HIVE, "partitioned_by", "external_location");
    }

    @Override
    protected void validateDataFiles(String partitionColumn, String tableName, String location)
    {
        getActiveFiles(tableName).forEach(dataFile ->
        {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            String partitionPart = partitionColumn.isEmpty() ? "" : partitionColumn + "=[a-z0-9]+/";
            assertThat(dataFile).matches("^" + Pattern.quote(locationDirectory) + partitionPart + "[a-zA-Z0-9_-]+$");
            verifyPathExist(dataFile);
        });
    }

    @Override
    protected void validateMetadataFiles(String location)
    {
        // No metadata files for Hive
    }

    @Override
    protected Set<String> getAllDataFilesFromTableDirectory(String tableLocation)
    {
        return new HashSet<>(getTableFiles(tableLocation));
    }

    @Override
    protected void validateFilesAfterOptimize(String location, Set<String> initialFiles, Set<String> updatedFiles)
    {
        assertThat(updatedFiles).hasSizeLessThan(initialFiles.size());
        assertThat(getAllDataFilesFromTableDirectory(location)).isEqualTo(updatedFiles);
    }

    @Override // Row-level modifications are not supported for Hive tables
    @ParameterizedTest
    @MethodSource("locationPatternsDataProvider")
    public void testBasicOperationsWithProvidedTableLocation(boolean partitioned, String locationPattern)
    {
        String tableName = "test_basic_operations_" + randomNameSuffix();
        String location = locationPattern.formatted(bucketName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioned_by = ARRAY['col_int']" : "");

        String create = "CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (external_location = '" + location + "'" + partitionQueryPart + ") " +
                "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)";
        if (locationPattern.contains("//double_slash/")) {
            assertQueryFails(create, "\\QUnsupported location that cannot be internally represented: " + location);
            return;
        }
        assertUpdate(create, 3);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3)");

        String actualTableLocation = getTableLocation(tableName);
        assertThat(actualTableLocation).isEqualTo(location);

        assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

        assertThat(getTableFiles(actualTableLocation)).isNotEmpty();
        validateDataFiles(partitioned ? "col_int" : "", tableName, actualTableLocation);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override // Row-level modifications are not supported for Hive tables
    @ParameterizedTest
    @MethodSource("locationPatternsDataProvider")
    public void testBasicOperationsWithProvidedSchemaLocation(boolean partitioned, String locationPattern)
    {
        String schemaName = "test_basic_operations_schema_" + randomNameSuffix();
        String schemaLocation = locationPattern.formatted(bucketName, schemaName);
        String tableName = "test_basic_operations_table_" + randomNameSuffix();
        String qualifiedTableName = schemaName + "." + tableName;
        String partitionQueryPart = (partitioned ? " WITH (partitioned_by = ARRAY['col_int'])" : "");

        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = '" + schemaLocation + "')");
        assertThat(getSchemaLocation(schemaName)).isEqualTo(schemaLocation);

        assertUpdate("CREATE TABLE " + qualifiedTableName + "(col_str varchar, col_int int)" + partitionQueryPart);
        String expectedTableLocation = (schemaLocation.endsWith("/") ? schemaLocation : schemaLocation + "/") + tableName;

        String actualTableLocation = metastore.getMetastore().getTable(schemaName, tableName).orElseThrow().storage().location();
        assertThat(actualTableLocation).isEqualTo(expectedTableLocation);

        assertUpdate("INSERT INTO " + qualifiedTableName + "  VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);
        assertQuery("SELECT * FROM " + qualifiedTableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3)");

        assertThat(getTableFiles(actualTableLocation)).isNotEmpty();
        validateDataFiles(partitioned ? "col_int" : "", qualifiedTableName, actualTableLocation);

        assertUpdate("DROP TABLE " + qualifiedTableName);
        assertThat(getTableFiles(actualTableLocation)).isEmpty();

        assertUpdate("DROP SCHEMA " + schemaName);
        validateFilesAfterDrop(actualTableLocation);
    }

    @Override
    @ParameterizedTest
    @MethodSource("locationPatternsDataProvider")
    public void testMergeWithProvidedTableLocation(boolean partitioned, String locationPattern)
    {
        // Row-level modifications are not supported for Hive tables
    }

    @ParameterizedTest
    @MethodSource("locationPatternsDataProvider")
    public void testAnalyzeWithProvidedTableLocation(boolean partitioned, String locationPattern)
    {
        String tableName = "test_analyze_" + randomNameSuffix();
        String location = locationPattern.formatted(bucketName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioned_by = ARRAY['col_int']" : "");

        String create = "CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (external_location = '" + location + "'" + partitionQueryPart + ") " +
                "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)";
        if (locationPattern.contains("//double_slash/")) {
            assertQueryFails(create, "\\QUnsupported location that cannot be internally represented: " + location);
            return;
        }
        assertUpdate(create, 3);

        assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

        String expectedPartitionedStatistics = """
                VALUES
                ('col_str', 16.0, 1.0, 0.0, null, null, null),
                ('col_int', null, 4.0, 0.0, null, 1, 4),
                (null, null, null, null, 4.0, null, null)""";

        //Check statistics collection on write
        if (partitioned) {
            assertQuery("SHOW STATS FOR " + tableName, expectedPartitionedStatistics);
        }
        else {
            assertQuery("SHOW STATS FOR " + tableName, """
                    VALUES
                    ('col_str', 16.0, 3.0, 0.0, null, null, null),
                    ('col_int', null, 3.0, 0.0, null, 1, 4),
                    (null, null, null, null, 4.0, null, null)""");
        }

        //Check statistics collection explicitly
        assertUpdate("ANALYZE " + tableName, 4);

        if (partitioned) {
            assertQuery("SHOW STATS FOR " + tableName, expectedPartitionedStatistics);
        }
        else {
            assertQuery("SHOW STATS FOR " + tableName, """
                    VALUES
                    ('col_str', 16.0, 4.0, 0.0, null, null, null),
                    ('col_int', null, 4.0, 0.0, null, 1, 4),
                    (null, null, null, null, 4.0, null, null)""");
        }

        assertUpdate("DROP TABLE " + tableName);
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

        assertThat(schemaLocation).isEqualTo("s3://" + bucketName + "/galaxy/..%2Ftest_create_schema_escaped_" + schemaNameSuffix);
        assertThat(tableLocation).isEqualTo("s3://" + bucketName + "/galaxy/..%2Ftest_create_schema_escaped_" + schemaNameSuffix + "/" + tableName);

        assertUpdate("DROP TABLE \"" + schemaName + "\"." + tableName);
        assertUpdate("DROP SCHEMA \"" + schemaName + "\"");
    }

    @Test
    public void testDotsSchemaNameEscape()
    {
        assertThatThrownBy(() -> assertUpdate("CREATE SCHEMA \"..\""))
                .hasMessage("Invalid schema name");
    }

    @Override
    @ParameterizedTest
    @MethodSource("locationPatternsDataProvider")
    public void testOptimizeWithProvidedTableLocation(boolean partitioned, String locationPattern)
    {
        abort("Objectstore connector does not support OPTIMIZE for Hive tables");
    }
}
