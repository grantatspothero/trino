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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestObjectStoreIcebergS3
        extends BaseObjectStoreS3Test
{
    public TestObjectStoreIcebergS3()
    {
        super(ICEBERG, "partitioning", "location");
    }

    @Override
    protected void validateDataFiles(String partitionColumn, String tableName, String location)
    {
        getActiveFiles(tableName).forEach(dataFile ->
        {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            String partitionPart = partitionColumn.isEmpty() ? "" : partitionColumn + "=[a-z0-9]+/";
            assertThat(dataFile).matches("^" + Pattern.quote(locationDirectory) + "data/" + partitionPart + "[a-zA-Z0-9_-]+.parquet$");
            verifyPathExist(dataFile);
        });
    }

    @Override
    protected void validateMetadataFiles(String location)
    {
        getAllMetadataDataFilesFromTableDirectory(location).forEach(metadataFile ->
        {
            String locationDirectory = location.endsWith("/") ? location : location + "/";
            assertThat(metadataFile).matches("^" + Pattern.quote(locationDirectory) + "metadata/[a-zA-Z0-9_-]+.(avro|metadata.json|stats)$");
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

    @ParameterizedTest
    @MethodSource("locationPatternsDataProvider")
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
                ('col_str', 4e0, 0e0, null, null, null),
                ('col_int', 4e0, 0e0, null, '1', '4'),
                (null, null, null, 4e0, null, null)""";

        // Check extended statistics collection on write
        assertThat(query("SHOW STATS FOR " + tableName)).exceptColumns("data_size")
                .skippingTypesCheck()
                .matches(expectedStatistics);

        // drop stats
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS");
        // Check extended statistics collection explicitly
        assertUpdate("ANALYZE " + tableName);
        assertThat(query("SHOW STATS FOR " + tableName)).exceptColumns("data_size")
                .skippingTypesCheck()
                .matches(expectedStatistics);

        assertUpdate("DROP TABLE " + tableName);
    }
}
