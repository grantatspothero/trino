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
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Predicate;

import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.cacheDataPlanNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.chooseAlternativeNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.loadCachedDataPlanNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestCacheCommonSubqueries
        extends BasePlanTest
{
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema("tiny")
            // disable so join order is not changed in tests
            .setSystemProperty(JOIN_REORDERING_STRATEGY, "none")
            // prevent hash columns from being added
            .setSystemProperty(OPTIMIZE_HASH_GENERATION, "false")
            .build();
    private static final CacheColumnId NATIONKEY_COLUMN_ID = new CacheColumnId("nationkey:bigint");
    private static final CacheColumnId REGIONKEY_COLUMN_ID = new CacheColumnId("regionkey:bigint");

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.builder(TEST_SESSION)
                .withCacheConfig(new CacheConfig()
                        .setEnabled(true)
                        .setCacheSubqueriesEnabled(true))
                .build();

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1, false),
                ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testCacheCommonSubqueries()
    {
        PlanSignature signature = new PlanSignature(
                new SignatureKey("tiny:nation:0.01"),
                Optional.empty(),
                ImmutableList.of(REGIONKEY_COLUMN_ID, NATIONKEY_COLUMN_ID),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        NATIONKEY_COLUMN_ID, Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 5L), greaterThan(BIGINT, 10L)), false))),
                TupleDomain.all());
        assertPlan("""
                        SELECT * FROM
                        (SELECT regionkey FROM nation WHERE nationkey > 10)
                        UNION ALL
                        (SELECT regionkey FROM nation WHERE nationkey < 5)
                        """,
                anyTree(exchange(LOCAL,
                        chooseAlternativeNode(
                                // original subplan
                                strictProject(ImmutableMap.of("REGIONKEY_A", expression("REGIONKEY_A")),
                                        filter("NATIONKEY_A > BIGINT '10'",
                                                tableScan("nation", ImmutableMap.of("NATIONKEY_A", "nationkey", "REGIONKEY_A", "regionkey")))),
                                // store data in cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_A", expression("REGIONKEY_A")),
                                        filter("NATIONKEY_A > BIGINT '10'",
                                                cacheDataPlanNode(
                                                        strictProject(ImmutableMap.of("REGIONKEY_A", expression("REGIONKEY_A"), "NATIONKEY_A", expression("NATIONKEY_A")),
                                                                filter("NATIONKEY_A > BIGINT '10' OR NATIONKEY_A < BIGINT '5'",
                                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_A", "nationkey", "REGIONKEY_A", "regionkey"))))))),
                                // load data from cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_A", expression("REGIONKEY_A")),
                                        filter("NATIONKEY_A > BIGINT '10'",
                                                loadCachedDataPlanNode(signature, ImmutableMap.of(), "REGIONKEY_A", "NATIONKEY_A")))),
                        chooseAlternativeNode(
                                // original subplan
                                strictProject(ImmutableMap.of("REGIONKEY_B", expression("REGIONKEY_B")),
                                        filter("NATIONKEY_B < BIGINT '5'",
                                                tableScan("nation", ImmutableMap.of("NATIONKEY_B", "nationkey", "REGIONKEY_B", "regionkey")))),
                                // store data in cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_B", expression("REGIONKEY_B")),
                                        filter("NATIONKEY_B < BIGINT '5'",
                                                cacheDataPlanNode(
                                                        strictProject(ImmutableMap.of("REGIONKEY_B", expression("REGIONKEY_B"), "NATIONKEY_B", expression("NATIONKEY_B")),
                                                                filter("NATIONKEY_B > BIGINT '10' OR NATIONKEY_B < BIGINT '5'",
                                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_B", "nationkey", "REGIONKEY_B", "regionkey"))))))),
                                // load data from cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_B", expression("REGIONKEY_B")),
                                        filter("NATIONKEY_B < BIGINT '5'",
                                                loadCachedDataPlanNode(signature, ImmutableMap.of(), "REGIONKEY_B", "NATIONKEY_B")))))));
    }

    @Test
    public void testJoinQuery()
    {
        PlanSignature signature = new PlanSignature(
                new SignatureKey("tiny:nation:0.01"),
                Optional.empty(),
                ImmutableList.of(NATIONKEY_COLUMN_ID, REGIONKEY_COLUMN_ID),
                TupleDomain.all(),
                TupleDomain.all());
        Predicate<FilterNode> isNationKeyDynamicFilter = node -> DynamicFilters.getDescriptor(node.getPredicate())
                .map(descriptor -> descriptor.getInput().equals(new SymbolReference("nationkey")))
                .orElse(false);
        assertPlan("""
                        SELECT * FROM
                        (SELECT nationkey FROM nation)
                        JOIN
                        (SELECT regionkey FROM nation)
                        ON nationkey = regionkey
                        """,
                anyTree(node(JoinNode.class,
                        chooseAlternativeNode(
                                // original subplan
                                filter(TRUE_LITERAL, // for DF on nationkey
                                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey")))
                                        .with(FilterNode.class, isNationKeyDynamicFilter),
                                // store data in cache alternative
                                strictProject(ImmutableMap.of("NATIONKEY", expression("NATIONKEY")),
                                        cacheDataPlanNode(
                                                filter(TRUE_LITERAL, // for DF on nationkey
                                                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey")))
                                                        .with(FilterNode.class, isNationKeyDynamicFilter))),
                                // load data from cache alternative
                                strictProject(ImmutableMap.of("NATIONKEY", expression("NATIONKEY")),
                                        loadCachedDataPlanNode(
                                                signature,
                                                ImmutableMap.of(new TpchColumnHandle("nationkey", BIGINT), NATIONKEY_COLUMN_ID),
                                                "NATIONKEY", "REGIONKEY"))),
                        anyTree(
                                chooseAlternativeNode(
                                        // original subplan
                                        tableScan("nation", ImmutableMap.of("REGIONKEY", "regionkey")),
                                        // store data in cache alternative
                                        strictProject(ImmutableMap.of("REGIONKEY", expression("REGIONKEY")),
                                                cacheDataPlanNode(
                                                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey")))),
                                        // load data from cache alternative
                                        strictProject(ImmutableMap.of("REGIONKEY", expression("REGIONKEY")),
                                                loadCachedDataPlanNode(signature, ImmutableMap.of(), "NATIONKEY", "REGIONKEY")))))));
    }
}
