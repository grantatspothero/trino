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
package io.trino.plugin.iceberg.catalog.galaxy;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.Column;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestGalaxyCatalogColumnMetadataCache
{
    private final TypeManager typeManager = new TestingTypeManager();

    @ParameterizedTest
    @MethodSource("trueFalse")
    public void testCacheColumnMetadata(boolean createNullableColumn)
    {
        Types.NestedField icebergColumn;
        if (createNullableColumn) {
            icebergColumn = Types.NestedField.optional(0, "name", Types.IntegerType.get(), "comment");
        }
        else {
            icebergColumn = Types.NestedField.required(0, "name", Types.IntegerType.get(), "comment");
        }

        Column metastoreRepresentation = GalaxyMetastoreTableOperations.toMetastoreColumn(icebergColumn, typeManager);
        ColumnMetadata trinoRepresentation = TrinoGalaxyCatalog.fromMetastoreCache(metastoreRepresentation, typeManager);
        assertEquals(
                trinoRepresentation,
                ColumnMetadata.builder()
                        .setName("name")
                        .setType(INTEGER)
                        .setNullable(createNullableColumn)
                        .setComment(Optional.of("comment"))
                        .setExtraInfo(Optional.empty())
                        .setHidden(false)
                        .setProperties(ImmutableMap.of())
                        .build());
    }

    private static Object[][] trueFalse()
    {
        return new Object[][] {{true}, {false}};
    }
}
