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
package io.trino.plugin.hive;

import io.trino.Session;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.regex.Pattern;

import static io.trino.SystemSessionProperties.USE_TABLE_SCAN_NODE_PARTITIONING;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestHiveWithPlanAlternativesConnectorTest
        extends BaseHiveConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BaseHiveConnectorTest.createHiveQueryRunner(HiveQueryRunner.builder()
                .withPlanAlternatives());
    }

    @Test
    public void testBucketedSelectWithFilter()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_bucketed_select\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                            "SELECT orderkey key1, comment value1, custkey FROM orders",
                    15000);
            Session planWithTableNodePartitioning = Session.builder(getSession())
                    .setSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, "true")
                    .build();
            Session planWithoutTableNodePartitioning = Session.builder(getSession())
                    .setSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, "false")
                    .build();

            @Language("SQL") String query = "SELECT key1, count(value1) FROM test_bucketed_select WHERE custkey > 5 GROUP BY key1";
            @Language("SQL") String expectedQuery = "SELECT orderkey, count(comment) FROM orders WHERE custkey > 5 GROUP BY orderkey";

            assertQuery(planWithTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(0));
            assertQuery(planWithoutTableNodePartitioning, query, expectedQuery, assertRemoteExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_bucketed_select");
        }
    }

    @Test
    public void testExplain()
    {
        assertExplain(
                "EXPLAIN SELECT name FROM nation WHERE nationkey = 8",
                "ChooseAlternativeNode",
                "ScanProject",  // filter is subsumed by the connector
                Pattern.quote("\"nationkey\" = BIGINT '8'"),  // filter is not subsumed by the connector
                "Estimates: \\{rows: .* \\(.*\\), cpu: .*, memory: .*, network: .*}",
                "Trino version: .*");
    }

    @Override
    public void testExplainAnalyzeScanFilterProjectWallTime()
    {
        // 'Filter CPU time' is not expected when connector uses an alternative in which the filter is subsumed
    }

    @Override
    @Test(dataProvider = "bucketFilteringDataTypesSetupProvider")
    public void testFilterOnBucketedTable(BucketedFilterTestSetup testSetup)
    {
        String tableName = "test_filter_on_bucketed_table_" + randomNameSuffix();
        assertUpdate(
                """
                CREATE TABLE %s (bucket_key %s, other_data double)
                WITH (
                    format = 'TEXTFILE',
                    bucketed_by = ARRAY[ 'bucket_key' ],
                    bucket_count = 5)
                """.formatted(tableName, testSetup.getTypeName()));

        String values = testSetup.getValues().stream()
                .map(value -> "(" + value + ", rand())")
                .collect(joining(", "));
        assertUpdate("INSERT INTO " + tableName + " VALUES " + values, testSetup.getValues().size());

        // It will only read data from a single bucket instead of all buckets,
        // so physicalInputPositions should be less than number of rows inserted (.
        assertQueryStats(
                getSession(),
                """
                SELECT count(*)
                FROM %s
                WHERE bucket_key = %s
                """.formatted(tableName, testSetup.getFilterValue()),
                // might be less than expected when using an alternative in which the filter is subsumed
                queryStats -> assertThat(queryStats.getPhysicalInputPositions()).isLessThanOrEqualTo(testSetup.getExpectedPhysicalInputRows()),
                result -> assertThat(result.getOnlyValue()).isEqualTo(testSetup.getExpectedResult()));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testMultipleWriters()
    {
        // Not applicable with plan alternatives
    }

    @Override
    public void testMultipleWritersWithSkewedData()
    {
        // Not applicable with plan alternatives
    }

    @Override
    public void testMultipleWritersWhenTaskScaleWritersIsEnabled()
    {
        // Not applicable with plan alternatives
    }

    @Override
    public void testTaskWritersDoesNotScaleWithLargeMinWriterSize()
    {
        // Not applicable with plan alternatives
    }

    @Override
    public void testWritersAcrossMultipleWorkersWhenScaleWritersIsEnabled()
    {
        // Not applicable with plan alternatives
    }

    @Override
    public void testWriterTasksCountLimitUnpartitioned(boolean scaleWriters, boolean redistributeWrites, int expectedFilesCount)
    {
        // Not applicable with plan alternatives
    }

    @Override
    public void testWriterTasksCountLimitPartitionedScaleWritersDisabled()
    {
        // Not applicable with plan alternatives
    }

    @Override
    public void testWriterTasksCountLimitPartitionedScaleWritersEnabled()
    {
        // Not applicable with plan alternatives
    }

    @Override
    public void testTargetMaxFileSize()
    {
        // Not applicable with plan alternatives
    }

    @Override
    public void testTargetMaxFileSizePartitioned()
    {
        // Not applicable with plan alternatives
    }

    @Override
    public void testTimestampWithTimeZone()
    {
        // There's no clean way to access HiveMetastoreFactory and this test doesn't exercise plan alternatives anyway
    }

    @Override
    public void testCoercingVarchar0ToVarchar1()
    {
        // TODO (https://github.com/starburstdata/galaxy-trino/issues/1124) Implement getSupportedType for objectstore
        assertThatThrownBy(super::testCoercingVarchar0ToVarchar1)
                .hasMessage("invalid varchar length: 0")
                .hasStackTraceContaining("at io.trino.plugin.hive.type.VarcharTypeInfo.<init>("); // failure inside Hive connector
    }

    @Override
    public void testCoercingVarchar0ToVarchar1WithCTAS()
    {
        // TODO (https://github.com/starburstdata/galaxy-trino/issues/1124) Implement getSupportedType for objectstore
        assertThatThrownBy(super::testCoercingVarchar0ToVarchar1WithCTAS)
                .hasMessage("invalid varchar length: 0")
                .hasStackTraceContaining("at io.trino.plugin.hive.type.VarcharTypeInfo.<init>("); // failure inside Hive connector
    }

    @Override
    public void testCoercingVarchar0ToVarchar1WithCTASNoData()
    {
        // TODO (https://github.com/starburstdata/galaxy-trino/issues/1124) Implement getSupportedType for objectstore
        assertThatThrownBy(super::testCoercingVarchar0ToVarchar1WithCTASNoData)
                .hasMessage("invalid varchar length: 0")
                .hasStackTraceContaining("at io.trino.plugin.hive.type.VarcharTypeInfo.<init>("); // failure inside Hive connector
    }
}
