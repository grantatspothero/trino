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
package io.trino.tests.product.warp;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import io.trino.testing.minio.MinioClient;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED;
import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.QueryExecutors.onHudi;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWarpSpeed
        extends ProductTest
{
    private static final Logger log = Logger.get(TestWarpSpeed.class);
    private static final String HUDI_TABLE_TYPE_COPY_ON_WRITE = "cow";
    private static final String QUERY_JMX_TABLE = "jmx.current.\"*pagesource*\"";
    private static final String WARM_JMX_TABLE = "jmx.current.\"*warm*\"";
    private final Map<String, String[]> jmxTableToColumns = Map.of(
            QUERY_JMX_TABLE,
            new String[] {
                    "varada_collect_columns",
                    "varada_match_columns",
                    "external_collect_columns",
                    "external_match_columns"},
            WARM_JMX_TABLE,
            new String[] {
                    "warm_accomplished",
                    "warmup_elements_count",
                    "row_group_count",
                    "warm_failed"});
    private MinioClient client;
    private String bucketName;

    @BeforeTestWithContext
    public void setUp()
            throws Exception
    {
        client = new MinioClient();
        client.ensureBucketExists("testing");
        bucketName = System.getenv().getOrDefault("S3_BUCKET", "trino-ci-test");
    }

    @AfterTestWithContext
    public void tearDown()
    {
        client.close();
        client = null;
    }

    @Test(groups = {WARP_SPEED, PROFILE_SPECIFIC_TESTS})
    public void testWarpHiveQuery()
    {
        String fullyQualifiedName = "warp" + "." + "default" + "." + "nation";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + fullyQualifiedName);

        QueryResult queryResult = onTrino().executeQuery("CREATE TABLE " + fullyQualifiedName + " AS SELECT * FROM tpch.tiny.nation");
        QueryAssert.assertThat(queryResult).containsOnly(row(25));
        String query = "SELECT * FROM " + fullyQualifiedName + " WHERE nationkey > 1";
        int expectedRowsCountResult = 23;
        testQueryWithRetry(query, WARM_JMX_TABLE, expectedRowsCountResult, new long[] {1L, 5L, 1L, 0L});
        testQueryWithRetry(query, QUERY_JMX_TABLE, expectedRowsCountResult, new long[] {4L, 1L, 0L, 0L});

        onTrino().executeQuery("DROP TABLE " + fullyQualifiedName);
    }

    @Test(groups = {WARP_SPEED, PROFILE_SPECIFIC_TESTS})
    public void testWarpHudiQuery()
    {
        String tableName = "hudi_insert_" + randomNameSuffix();
        String schemaTableName = schemaTableName("default", tableName);
        createHudiTable(schemaTableName);
        String warpTableName = schemaTableName("warp", schemaTableName);
        String query = "SELECT * FROM " + warpTableName;
        int expectedRowsCountResult = 2;
        testQueryWithRetry(query, WARM_JMX_TABLE, expectedRowsCountResult, new long[] {1L, 9L, 1L, 0L});
        testQueryWithRetry(query, QUERY_JMX_TABLE, expectedRowsCountResult, new long[] {9L, 0L, 0L, 0L});
    }

    private void testQueryWithRetry(@Language("SQL") String query, String queryJmxTable, int expectedRowsCountResult, long[] expectedValues)
    {
        String[] statsColumns = jmxTableToColumns.get(queryJmxTable);
        QueryResult materializedRow = getServiceStats(queryJmxTable, statsColumns);
        List<Long> beforeValues = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < statsColumns.length + 1; i++) {
            beforeValues.add((Long) materializedRow.column(i).get(0));
        }
        sb.append("before stats: ");
        for (int i = 0; i < statsColumns.length; i++) {
            sb.append(format("%s=%s , ", statsColumns[i], beforeValues.get(i)));
        }
        log.info("execute query=\"%s\" stats before=%s", query, sb.toString());
        QueryResult queryResult = onTrino().executeQuery(query);
        assertThat(queryResult.rows().size()).isEqualTo(expectedRowsCountResult);
        assertEventually(Duration.valueOf("20s"),
                () -> {
                    // warm may take some time to take action, set duration so 20s.
                    QueryResult materializedRowAfter = getServiceStats(queryJmxTable, statsColumns);
                    for (int i = 0; i < statsColumns.length; i++) {
                        Long actualValue = (Long) materializedRowAfter.column(i + 1).get(0) - beforeValues.get(i);
                        assertThat(actualValue)
                                .describedAs(format("%s is not as expected", statsColumns[i]))
                                .isEqualTo(expectedValues[i]);
                    }
                });
        log.info("finished validation stats for %s table", queryJmxTable);
    }

    private QueryResult getServiceStats(String jmxTable, String... statColNames)
    {
        String statSumColNames = Stream.of(statColNames)
                .map(s -> "sum(" + s + ")")
                .collect(Collectors.joining(","));
        String format = "SELECT " + statSumColNames + "FROM " + jmxTable;
        return onTrino().executeQuery(format);
    }

    private void createHudiTable(String tableName)
    {
        createHudiNonPartitionedTable(tableName, bucketName);
    }

    private void createHudiNonPartitionedTable(String tableName, String bucketName)
    {
        onHudi().executeQuery(format(
                """
                        CREATE TABLE %s (
                          id bigint,
                          name string,
                          price int,
                          ts bigint)
                        USING hudi
                        TBLPROPERTIES (
                          type = '%s',
                          primaryKey = 'id',
                          preCombineField = 'ts')
                        LOCATION 's3://%s/%s'""",
                tableName,
                TestWarpSpeed.HUDI_TABLE_TYPE_COPY_ON_WRITE,
                bucketName,
                tableName));

        onHudi().executeQuery("INSERT INTO " + tableName + " VALUES (1, 'a1', 20, 1000), (2, 'a2', 40, 2000)");
    }

    private String schemaTableName(String schema, String tableName)
    {
        return "%s.%s".formatted(schema, tableName);
    }
}
