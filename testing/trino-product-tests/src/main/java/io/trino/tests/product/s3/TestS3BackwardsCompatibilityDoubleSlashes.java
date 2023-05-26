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
package io.trino.tests.product.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.airlift.log.Logger;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryExecutor;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.S3_BACKWARDS_COMPATIBILITY;
import static io.trino.tests.product.utils.QueryExecutors.connectToTrino;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestS3BackwardsCompatibilityDoubleSlashes
        extends ProductTest
{
    private static final Logger log = Logger.get(TestS3BackwardsCompatibilityDoubleSlashes.class);

    private Closer closer;
    private AmazonS3 s3;
    private String schemaName;

    @BeforeTestWithContext
    public void setup()
    {
        closer = Closer.create();
        s3 = AmazonS3Client.builder().build();
        closer.register(s3::shutdown);

        schemaName = "test_s3_backwards_compatibility_" + randomNameSuffix();
        // Create schema with double slashes in the location. This will get inherited by the table locations.
        // This is the case that used to be erroneously handled by previous Trino versions, leading to table corruption.
        // This test verifies we can still read from and operate on such tables.
        onTrino().executeQuery("CREATE SCHEMA delta." + schemaName + " WITH (location = 's3://galaxy-trino-ci/temp_files//" + schemaName + "')");
        closer.register(() -> onTrino().executeQuery("DROP SCHEMA delta." + schemaName));
        closer.register(() -> {
            onTrino().executeQuery("SHOW TABLES FROM delta." + schemaName).column(1).stream()
                    .map(String.class::cast)
                    .forEach(tableName -> {
                        log.warn("Table '%s' left behind after test method execution, trying to remove it", tableName);
                        for (String catalogName : List.of("iceberg", "delta")) {
                            try {
                                onTrino().executeQuery("DROP TABLE %s.%s.%s".formatted(catalogName, schemaName, tableName));
                                return;
                            }
                            catch (QueryExecutionException ignored) {
                            }
                        }
                        for (String catalogName : List.of("iceberg", "delta")) {
                            try {
                                onTrino().executeQuery("CALL %s.system.unregister_table(schema_name => '%s', table_name => '%s')".formatted(catalogName, schemaName, tableName));
                                return;
                            }
                            catch (QueryExecutionException ignored) {
                            }
                        }
                        log.error("Failed to remove table '%s' left behind after test method execution", tableName);
                    });
        });
    }

    @AfterTestWithContext
    public void tearDown()
            throws Exception
    {
        if (closer != null) {
            closer.close();
            closer = null;
        }
        s3 = null;
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testContainerVersions()
    {
        assertThat(onTrino415().executeQuery("SELECT version()"))
                .containsOnly(row("415"));
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY}, dataProvider = "tableFormats")
    public void testReadsOnCorruptedTable(String tableFormat)
    {
        String tableName = tableFormat + "_reads_on_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "%s.%s.%s".formatted(tableFormat, schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1)");
            assertThat(onTrino415().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1));
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY}, dataProvider = "tableFormats")
    public void testInsertsOnCorruptedTable(String tableFormat)
    {
        String tableName = tableFormat + "_writes_on_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "%s.%s.%s".formatted(tableFormat, schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a, b) AS VALUES (1, 2)");
        try {
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (3, 4)");
            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (5, 6)");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(1, 2), row(3, 4), row(5, 6));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testDeletesOnCorruptedIcebergTable()
    {
        String tableName = "iceberg_deletes_on_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "iceberg.%s.%s".formatted(schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1), (2, 3)");
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (4, 5)");
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13)");
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (14, 15)");

            onTrino415().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 2");
            onTrino415().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 4");
            assertThat(onTrino415().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(10, 11), row(12, 13), row(14, 15));

            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 12");
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 14");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(10, 11));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY}, dataProvider = "tableFormats")
    public void testDropTableRemovesCorruptedFiles(String tableFormat)
    {
        String tableName = tableFormat + "_drop_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "%s.%s.%s".formatted(tableFormat, schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a, b) AS VALUES (1, 2), (3, 4)");
        try {
            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (5, 6)");

            // The actual table location may be randomized, and then this would be just a prefix. In any case, it is unique for this table since table name is randomized as well.
            String tableLocationPrefixWithDoubleSlash = "temp_files//" + schemaName + "/" + tableName;
            String tableLocationPrefixWithoutDoubleSlash = "temp_files/" + schemaName + "/" + tableName;
            ListObjectsV2Request listObjectsRequestWithDoubleSlash = new ListObjectsV2Request()
                    .withBucketName("galaxy-trino-ci")
                    .withPrefix(tableLocationPrefixWithDoubleSlash);
            ListObjectsV2Request listObjectsRequestWithoutDoubleSlash = new ListObjectsV2Request()
                    .withBucketName("galaxy-trino-ci")
                    .withPrefix(tableLocationPrefixWithoutDoubleSlash);

            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithDoubleSlash).getObjectSummaries()).isNotEmpty();
            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries()).isNotEmpty();
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithDoubleSlash).getObjectSummaries()).isEmpty();
            if (!tableFormat.equals("iceberg")) {
                Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries()).isEmpty();
            }
            else {
                // TODO Iceberg with Glue catalog leaves stats file behind on DROP TABLE.
                // As seen in testVerifyLegacyDropTableBehavior, this is as least is not a regression.
                Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries())
                        .hasSize(1)
                        .singleElement().extracting(S3ObjectSummary::getKey, InstanceOfAssertFactories.STRING)
                        .matches("(temp_files)/(" + schemaName + ")/(" + tableName + "(?:-[a-z0-9]+)?)/(metadata)/([-a-z0-9_]+\\.stats)" +
                                "#%2F\\1%2F%2F\\2%2F\\3%2F\\4%2F\\5");
            }
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + qualifiedTableName);
        }
    }

    /**
     * Like {@link #testDropTableRemovesCorruptedFiles} but doesn't write with new version & drops with {@link #onTrino415()}.
     * This serves for documentation purposes.
     */
    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY}, dataProvider = "tableFormats")
    public void testVerifyLegacyDropTableBehavior(String tableFormat)
    {
        String tableName = tableFormat + "_legacy_drop_corrupted_table_" + randomNameSuffix();
        String qualifiedTableName = "%s.%s.%s".formatted(tableFormat, schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a, b) AS VALUES (1, 2), (3, 4)");
        try {
            // The actual table location may be randomized, and then this would be just a prefix. In any case, it is unique for this table since table name is randomized as well.
            String tableLocationPrefixWithDoubleSlash = "temp_files//" + schemaName + "/" + tableName;
            String tableLocationPrefixWithoutDoubleSlash = "temp_files/" + schemaName + "/" + tableName;
            ListObjectsV2Request listObjectsRequestWithDoubleSlash = new ListObjectsV2Request()
                    .withBucketName("galaxy-trino-ci")
                    .withPrefix(tableLocationPrefixWithDoubleSlash);
            ListObjectsV2Request listObjectsRequestWithoutDoubleSlash = new ListObjectsV2Request()
                    .withBucketName("galaxy-trino-ci")
                    .withPrefix(tableLocationPrefixWithoutDoubleSlash);

            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithDoubleSlash).getObjectSummaries()).isEmpty(); // old version didn't write anything here
            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries()).isNotEmpty();
            onTrino415().executeQuery("DROP TABLE " + qualifiedTableName);
            Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithDoubleSlash).getObjectSummaries()).isEmpty();
            if (!tableFormat.equals("iceberg")) {
                Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries()).isEmpty();
            }
            else {
                // Trino 415's Iceberg with Glue catalog would leave stats file behind on DROP TABLE
                Assertions.assertThat(s3.listObjectsV2(listObjectsRequestWithoutDoubleSlash).getObjectSummaries())
                        .hasSize(1)
                        .singleElement().extracting(S3ObjectSummary::getKey, InstanceOfAssertFactories.STRING)
                        .matches("(temp_files)/(" + schemaName + ")/(" + tableName + "(?:-[a-z0-9]+)?)/(metadata)/([-a-z0-9_]+\\.stats)" +
                                "#%2F\\1%2F%2F\\2%2F\\3%2F\\4%2F\\5");
            }
        }
        finally {
            onTrino415().executeQuery("DROP TABLE IF EXISTS " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testRowLevelDeletesOnCorruptedDeltaTable()
    {
        String tableName = "delta_row_level_deletes_" + randomNameSuffix();
        String qualifiedTableName = "delta.%s.%s".formatted(schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1), (2, 3), (4, 5)");

            // 415 could not perform row level deletes on files created by 415
            assertQueryFailure(() -> onTrino415().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 4"))
                    .hasMessageContaining("Unable to rewrite Parquet file");
            assertThat(onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 4")).containsOnly(row(1));

            // Should be able to delete from uncorrupted files on new versions
            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13), (14, 15)");
            assertThat(onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 14")).containsOnly(row(1));
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 3), row(10, 11), row(12, 13));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testRowLevelDeletesOnCorruptedIcebergTable()
    {
        String tableName = "iceberg_row_level_deletes_" + randomNameSuffix();
        String qualifiedTableName = "iceberg.%s.%s".formatted(schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1), (2, 3), (4, 5)");

            // Both versions can delete from files created by 415
            onTrino415().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 4");
            assertThat(onTrino415().executeQuery("TABLE " + qualifiedTableName)).containsOnly(row(0, 1), row(2, 3));
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName)).containsOnly(row(0, 1), row(2, 3));
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 2");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName)).containsOnly(row(0, 1));

            // Uncorrupted files can be modified by new versions
            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13), (14, 15)");
            onTrino().executeQuery("DELETE FROM " + qualifiedTableName + " WHERE a = 14");

            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(10, 11), row(12, 13));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testUpdatesOnCorruptedDeltaTable()
    {
        String tableName = "delta_update_" + randomNameSuffix();
        String qualifiedTableName = "delta.%s.%s".formatted(schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1), (2, 3), (4, 5)");
            // 415 could not update files created by 415
            assertQueryFailure(() -> onTrino415().executeQuery("UPDATE " + qualifiedTableName + " SET b = 42 WHERE a = 4"))
                    .hasMessageContaining("Unable to rewrite Parquet file");
            assertThat(onTrino().executeQuery("UPDATE " + qualifiedTableName + " SET b = 42 WHERE a = 4"))
                    .containsOnly(row(1));

            // Uncorrupted files should be updatable by new versions
            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13), (14, 15)");
            onTrino().executeQuery("UPDATE " + qualifiedTableName + " SET b = 42 WHERE a = 14");

            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 3), row(4, 42), row(10, 11), row(12, 13), row(14, 42));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testUpdatesOnCorruptedIcebergTable()
    {
        String tableName = "iceberg_update_" + randomNameSuffix();
        String qualifiedTableName = "iceberg.%s.%s".formatted(schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1), (2, 3), (4, 5)");

            // Both versions can delete from files created by 415
            onTrino415().executeQuery("UPDATE " + qualifiedTableName + " SET b = 25 WHERE a = 4");
            assertThat(onTrino415().executeQuery("TABLE " + qualifiedTableName)).containsOnly(row(0, 1), row(2, 3), row(4, 25));
            onTrino().executeQuery("UPDATE " + qualifiedTableName + " SET b = 23 WHERE a = 2");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName)).containsOnly(row(0, 1), row(2, 23), row(4, 25));

            // Uncorrupted files can be modified by new versions
            onTrino().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (10, 11), (12, 13), (14, 15)");
            onTrino().executeQuery("UPDATE " + qualifiedTableName + " SET b = 33 WHERE a = 12");

            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName))
                    .containsOnly(row(0, 1), row(2, 23), row(4, 25), row(10, 11), row(12, 33), row(14, 15));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testProceduresOnDelta()
    {
        String tableName = "delta_table_procedures_" + randomNameSuffix();
        String qualifiedTableName = "delta.%s.%s".formatted(schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1)");
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (2, 3)");
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (4, 5)");

            List<QueryAssert.Row> expected = ImmutableList.of(row(0, 1), row(2, 3), row(4, 5));
            onTrino().executeQuery("ALTER TABLE " + qualifiedTableName + " EXECUTE optimize");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName)).containsOnly(expected);

            onTrino().executeQuery("SET SESSION delta.vacuum_min_retention = '0s'");
            onTrino().executeQuery("CALL delta.system.vacuum('%s', '%s', '0s')".formatted(schemaName, tableName));

            String tableLocationPrefixWithDoubleSlash = "temp_files//" + schemaName + "/" + tableName;
            String tableLocationPrefixWithoutDoubleSlash = "temp_files/" + schemaName + "/" + tableName;

            List<S3ObjectSummary> dataFilesWithDoubleSlash = listPrefix(tableLocationPrefixWithDoubleSlash).stream()
                    .filter(summary -> !summary.getKey().contains("_delta_log"))
                    .collect(toImmutableList());
            List<S3ObjectSummary> dataFilesWithoutDoubleSlash = listPrefix(tableLocationPrefixWithoutDoubleSlash).stream()
                    .filter(summary -> !summary.getKey().contains("_delta_log"))
                    .collect(toImmutableList());

            // TODO: Vacuum does not clean up corrupted files
            Assertions.assertThat(dataFilesWithDoubleSlash).hasSize(1);
            Assertions.assertThat(dataFilesWithoutDoubleSlash).hasSize(3);
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS, S3_BACKWARDS_COMPATIBILITY})
    public void testProceduresOnIceberg()
    {
        String tableName = "iceberg_table_procedures_" + randomNameSuffix();
        String qualifiedTableName = "iceberg.%s.%s".formatted(schemaName, tableName);
        onTrino415().executeQuery("CREATE TABLE " + qualifiedTableName + "(a INT, b INT)");
        try {
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (0, 1)");
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (2, 3)");
            onTrino415().executeQuery("INSERT INTO " + qualifiedTableName + " VALUES (4, 5)");

            List<QueryAssert.Row> expected = ImmutableList.of(row(0, 1), row(2, 3), row(4, 5));
            onTrino().executeQuery("ALTER TABLE " + qualifiedTableName + " EXECUTE optimize");
            assertThat(onTrino().executeQuery("TABLE " + qualifiedTableName)).containsOnly(expected);

            onTrino().executeQuery("SET SESSION iceberg.expire_snapshots_min_retention = '0s'");
            onTrino().executeQuery("SET SESSION iceberg.remove_orphan_files_min_retention = '0s'");
            onTrino().executeQuery("ALTER TABLE %s EXECUTE expire_snapshots(retention_threshold => '0s')".formatted(qualifiedTableName));
            onTrino().executeQuery("ALTER TABLE %s EXECUTE remove_orphan_files(retention_threshold => '0s')".formatted(qualifiedTableName));

            String tableLocationPrefixWithDoubleSlash = "temp_files//%s/%s/data/".formatted(schemaName, tableName);
            String tableLocationPrefixWithoutDoubleSlash = "temp_files/%s/%s/data/".formatted(schemaName, tableName);

            // TODO: remove_orphan_files and expire_snapshots do not clean up corrupted files
            Assertions.assertThat(listPrefix(tableLocationPrefixWithDoubleSlash)).hasSize(1);
            Assertions.assertThat(listPrefix(tableLocationPrefixWithoutDoubleSlash)).hasSize(3);
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + qualifiedTableName);
        }
    }

    private List<S3ObjectSummary> listPrefix(String prefix)
    {
        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName("galaxy-trino-ci")
                .withPrefix(prefix);
        return s3.listObjectsV2(listObjectsRequest).getObjectSummaries();
    }

    @DataProvider
    public static Object[][] tableFormats()
    {
        return new Object[][] {
                // Hive not covered, since Trino 415 used normalized paths for Hive table location (no double slashes)
                {"iceberg"},
                {"delta"},
        };
    }

    private static QueryExecutor onTrino415()
    {
        return connectToTrino("trino-415");
    }
}
