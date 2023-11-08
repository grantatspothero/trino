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

import com.google.common.base.Stopwatch;
import io.trino.Session;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.union;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestObjectStoreDeltaS3
        extends BaseObjectStoreS3Test
{
    public TestObjectStoreDeltaS3()
    {
        super(DELTA, "partitioned_by", "location");
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
        String locationDirectory = location.endsWith("/") ? location : location + "/";
        getAllMetadataDataFilesFromTableDirectory(location).forEach(metadataFile ->
        {
            assertThat(metadataFile).matches("^" + Pattern.quote(locationDirectory) + "_delta_log/[0-9]+.json$");
            verifyPathExist(metadataFile);
        });

        assertThat(getExtendedStatisticsFileFromTableDirectory(location)).matches("^" + Pattern.quote(locationDirectory) + "_delta_log/_trino_meta/extended_stats.json$");
    }

    @Override
    protected void validateFilesAfterDrop(String location)
    {
        // In Delta table created with location in treated as external, so files are not removed
        assertThat(getTableFiles(location)).isNotEmpty();
    }

    @Override
    protected Set<String> getAllDataFilesFromTableDirectory(String tableLocation)
    {
        return getTableFiles(tableLocation).stream()
                .filter(path -> !path.contains("_delta_log"))
                .collect(Collectors.toUnmodifiableSet());
    }

    private Set<String> getAllMetadataDataFilesFromTableDirectory(String tableLocation)
    {
        return getTableFiles(tableLocation).stream()
                .filter(path -> path.contains("_delta_log") && !path.contains("/_trino_meta"))
                .collect(Collectors.toUnmodifiableSet());
    }

    private String getExtendedStatisticsFileFromTableDirectory(String tableLocation)
    {
        return getOnlyElement(getTableFiles(tableLocation).stream()
                .filter(path -> path.contains("/_trino_meta"))
                .collect(Collectors.toUnmodifiableSet()));
    }

    @ParameterizedTest
    @MethodSource("locationPatternsDataProvider")
    public void testAnalyzeWithProvidedTableLocation(boolean partitioned, String locationPattern)
    {
        String tableName = "test_analyze_" + randomNameSuffix();
        String location = locationPattern.formatted(bucketName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioned_by = ARRAY['col_str']" : "");

        assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (location = '" + location + "'" + partitionQueryPart + ") " +
                "AS VALUES ('str1', 1), ('str2', 2), ('str3', 3)", 3);

        assertUpdate("INSERT INTO " + tableName + " VALUES ('str4', 4)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2), ('str3', 3), ('str4', 4)");

        String expectedStatistics = """
                VALUES
                ('col_str', 16.0, 4.0, 0.0, null, null, null),
                ('col_int', null, 4.0, 0.0, null, 1, 4),
                (null, null, null, null, 4.0, null, null)""";

        String expectedPartitionedStatistics = """
                VALUES
                ('col_str', null, 4.0, 0.0, null, null, null),
                ('col_int', null, 4.0, 0.0, null, 1, 4),
                (null, null, null, null, 4.0, null, null)""";

        //Check extended statistics collection on write
        if (partitioned) {
            assertQuery("SHOW STATS FOR " + tableName, expectedPartitionedStatistics);
        }
        else {
            assertQuery("SHOW STATS FOR " + tableName, expectedStatistics);
        }

        // drop stats
        assertUpdate(format("CALL system.drop_extended_stats('%s', '%s')", TPCH_SCHEMA, tableName));

        //Check extended statistics collection explicitly
        assertUpdate("ANALYZE " + tableName, 4);

        if (partitioned) {
            assertQuery("SHOW STATS FOR " + tableName, expectedPartitionedStatistics);
        }
        else {
            assertQuery("SHOW STATS FOR " + tableName, expectedStatistics);
        }

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("locationPatternsDataProvider")
    public void testVacuum(boolean partitioned, String locationPattern)
            throws Exception
    {
        String tableName = "test_vacuum_" + randomNameSuffix();
        String tableLocation = locationPattern.formatted(bucketName, tableName);
        String partitionQueryPart = (partitioned ? ",partitioned_by = ARRAY['regionkey']" : "");

        String catalog = getSession().getCatalog().orElseThrow();
        Session sessionWithShortRetentionUnlocked = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "vacuum_min_retention", "0s")
                .build();
        assertUpdate("CREATE TABLE " + tableName +
                " WITH (location = '" + tableLocation + "'" + partitionQueryPart + ")" +
                " AS SELECT * FROM tpch.tiny.nation", 25);
        try {
            Set<String> initialFiles = getActiveFiles(tableName);

            computeActual("UPDATE " + tableName + " SET nationkey = nationkey + 100");
            Stopwatch timeSinceUpdate = Stopwatch.createStarted();
            Set<String> updatedFiles = getActiveFiles(tableName);
            assertThat(updatedFiles).doesNotContainAnyElementsOf(initialFiles);
            assertThat(getAllDataFilesFromTableDirectory(tableLocation)).isEqualTo(union(initialFiles, updatedFiles));

            // vacuum with high retention period, nothing should change
            assertUpdate(sessionWithShortRetentionUnlocked, "CALL system.vacuum(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "', retention => '10m')");
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("SELECT nationkey + 100, CAST(name AS varchar), regionkey, CAST(comment AS varchar) FROM tpch.tiny.nation");
            assertThat(getActiveFiles(tableName)).isEqualTo(updatedFiles);
            assertThat(getAllDataFilesFromTableDirectory(tableLocation)).isEqualTo(union(initialFiles, updatedFiles));

            // vacuum with low retention period
            MILLISECONDS.sleep(1_000 - timeSinceUpdate.elapsed(MILLISECONDS) + 1);
            assertUpdate(sessionWithShortRetentionUnlocked, "CALL system.vacuum(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "', retention => '1s')");
            // table data shouldn't change
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("SELECT nationkey + 100, CAST(name AS varchar), regionkey, CAST(comment AS varchar) FROM tpch.tiny.nation");
            // active files shouldn't change
            assertThat(getActiveFiles(tableName)).isEqualTo(updatedFiles);
            // old files should be cleaned up
            assertThat(getAllDataFilesFromTableDirectory(tableLocation)).isEqualTo(updatedFiles);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }
}
