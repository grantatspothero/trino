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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.cache.CacheController.SubplanKey;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.sql.tree.Expression;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.cache.CacheController.toSubplanKey;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheController
{
    @Test
    public void testToSubplanKey()
    {
        CacheTableId tableId = new CacheTableId("table_id");
        Expression predicateA = expression("a > b");
        Expression predicateB = expression("c");
        assertThat(toSubplanKey(tableId, Optional.empty(), ImmutableList.of(predicateA, predicateB)))
                .isEqualTo(new SubplanKey(tableId, Optional.empty(), ImmutableSet.of()));
        assertThat(toSubplanKey(tableId, Optional.of(ImmutableSet.of(new CacheColumnId("b"), new CacheColumnId("c"))), ImmutableList.of(predicateA, predicateB)))
                .isEqualTo(new SubplanKey(tableId, Optional.of(ImmutableSet.of(new CacheColumnId("b"), new CacheColumnId("c"))), ImmutableSet.of(predicateA)));
    }
}
