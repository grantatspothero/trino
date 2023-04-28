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

import io.trino.spi.connector.SchemaTableName;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class TestTableTypeCache
{
    @Test
    public void testDefaultToIceberg()
    {
        assertAffinity(new TableTypeCache(), new SchemaTableName("some_schema", "table_name"), TableType.ICEBERG);
    }

    @Test
    public void testDefaultToMostFrequent()
    {
        for (TableType tableType : TableType.values()) {
            TableType otherType = Arrays.stream(TableType.values())
                    .filter(other -> other != tableType)
                    .findFirst().orElseThrow();

            TableTypeCache tableTypeCache = new TableTypeCache();
            for (int i = 0; i < 5; i++) {
                tableTypeCache.record(new SchemaTableName("some_schema", "table_name" + i), tableType);
            }

            // it should not default to recently used type
            assertAffinity(tableTypeCache, new SchemaTableName("some_schema", "other_table"), tableType);

            // one table of a different type
            tableTypeCache.record(new SchemaTableName("some_schema", "different_type"), otherType);
            assertAffinity(tableTypeCache, new SchemaTableName("some_schema", "different_type"), otherType);
            assertAffinity(tableTypeCache, new SchemaTableName("some_schema", "yet_another_table"), tableType);
        }
    }

    private void assertAffinity(TableTypeCache tableTypeCache, SchemaTableName schemaTableName, TableType expected)
    {
        assertEquals(
                tableTypeCache.getTableTypeAffinity(schemaTableName).get(0),
                expected);
    }
}
