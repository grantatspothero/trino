/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

import io.trino.testing.AbstractTestDistributedQueries;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithMemory;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

// Extra Starburst Remote connector tests which require enabled writes and do not fit
// in TestStarburstRemoteDistributedQueriesWritesEnabled.
public class TestStarburstRemoteWithWritesEnabledExtraTests
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithMemory(
                Map.of(),
                TpchTable.getTables(),
                Optional.empty()));
        return createStarburstRemoteQueryRunner(
                true,
                Map.of(),
                Map.of(
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, "memory"),
                        "allow-drop-table", "true"));
    }

    @Test(dataProvider = "largeInValuesCount", dataProviderClass = AbstractTestDistributedQueries.class)
    public void testLargeInLongColumnName(int valuesCount)
    {
        String tableName = "test_large_column_name_" + randomTableSuffix();
        String columnName = "this_is_a_very_looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong_column_name";
        assertUpdate(format("CREATE TABLE %s (%s bigint)", tableName, columnName));

        String longValues = range(0, valuesCount)
                .mapToObj(Integer::toString)
                .collect(joining(", "));

        assertQuery(format("SELECT * FROM %s WHERE %s IN (%s)", tableName, columnName, longValues), "SELECT 1 WHERE 1=2");
        assertQuery(format("SELECT * FROM %s WHERE %s NOT IN (%s)", tableName, columnName, longValues), "SELECT 1 WHERE 1=2");

        assertQuery(format("SELECT * FROM %s WHERE %s IN (mod(1000, %s), %s)", tableName, columnName, columnName, longValues), "SELECT 1 WHERE 1=2");
        assertQuery(format("SELECT * FROM %s WHERE %s NOT IN (mod(1000, %s), %s)", tableName, columnName, columnName, longValues), "SELECT 1 WHERE 1=2");
    }
}