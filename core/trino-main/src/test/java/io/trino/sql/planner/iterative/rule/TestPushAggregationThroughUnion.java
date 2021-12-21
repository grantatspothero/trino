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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.UnionNode;
import org.testng.annotations.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPushAggregationThroughUnion
        extends BaseRuleTest
{
    @Test
    public void testPushAggregationThroughUnion()
    {
        tester().assertThat(new PushAggregationThroughUnion(tester().getMetadata()))
                .on(p -> {
                            Symbol a = p.symbol("a");
                            Symbol b = p.symbol("b");
                            Symbol c = p.symbol("c");
                            Symbol d = p.symbol("d");
                            UnionNode union = p.union(
                                    ImmutableListMultimap.<Symbol, Symbol>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(10, a),
                                            p.values(10, b)));
                            return p.aggregation(builder -> builder
                                    .globalGrouping()
                                    .addAggregation(d, expression("count(c)"), ImmutableList.of(BIGINT))
                                    .source(union));
                })
                .matches(
                        limit(1,
                                union(
                                        limit(1, ImmutableList.of(), true, values("a")),
                                        limit(1, ImmutableList.of(), true, values("b")))));
    }

    @Test
    public void testPushAggregationThroughUnionDoesNotFire()
    {
        tester().assertThat(new PushAggregationThroughUnion(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    UnionNode union = p.union(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(c, a)
                                    .put(c, b)
                                    .build(),
                            ImmutableList.of(
                                    p.values(10, a),
                                    p.values(10, b)));
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(d, expression("count(distinct c)"), ImmutableList.of(BIGINT))
                            .source(union));
                }).doesNotFire();
    }
}
