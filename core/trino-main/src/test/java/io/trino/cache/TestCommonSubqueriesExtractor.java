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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.cost.StatsAndCosts;
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.ExpressionExtractor;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cost.StatsCalculator.noopStatsCalculator;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCommonSubqueriesExtractor
        extends BasePlanTest
{
    private static final CacheTableId CACHE_TABLE_ID = new CacheTableId("cache_table_id");
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(TEST_SCHEMA)
            .build();
    private static final MockConnectorColumnHandle HANDLE_1 = new MockConnectorColumnHandle("column1", BIGINT);
    private static final MockConnectorColumnHandle HANDLE_2 = new MockConnectorColumnHandle("column2", BIGINT);

    private TableHandle testTableHandle;

    @Override
    protected LocalQueryRunner createLocalQueryRunner()
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION);
        queryRunner.createCatalog(
                TEST_CATALOG_NAME,
                MockConnectorFactory.builder()
                        .withGetColumns(handle -> ImmutableList.of(
                                new ColumnMetadata("column1", BIGINT),
                                new ColumnMetadata("column2", BIGINT)))
                        .withGetCacheTableId(handle -> Optional.of(CACHE_TABLE_ID))
                        .withGetCacheColumnId(handle -> {
                            MockConnectorColumnHandle column = (MockConnectorColumnHandle) handle;
                            return Optional.of(new CacheColumnId("cache_" + column.getName()));
                        })
                        .build(),
                ImmutableMap.of());
        testTableHandle = new TableHandle(
                queryRunner.getCatalogHandle(TEST_CATALOG_NAME),
                new MockConnectorTableHandle(new SchemaTableName(TEST_SCHEMA, TEST_TABLE)),
                TestingTransactionHandle.create());
        return queryRunner;
    }

    @Test
    public void testExtractCommonSubqueries()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);
        Symbol subqueryAColumn2 = symbolAllocator.newSymbol("subquery_a_column2", BIGINT);
        Symbol subqueryAProjection1 = symbolAllocator.newSymbol("subquery_a_projection1", BIGINT);
        // subquery A scans column1 and column2
        PlanNode scanA = new TableScanNode(
                new PlanNodeId("scanA"),
                testTableHandle,
                ImmutableList.of(subqueryAColumn1, subqueryAColumn2),
                ImmutableMap.of(subqueryAColumn1, HANDLE_1, subqueryAColumn2, HANDLE_2),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(true));
        // subquery A has complex predicate, but no DF
        FilterNode filterA = new FilterNode(
                new PlanNodeId("filterA"),
                scanA,
                expression("subquery_a_column1 > BIGINT '42' OR subquery_a_column2 % 2 = BIGINT '0'"));
        ProjectNode projectA = new ProjectNode(
                new PlanNodeId("projectA"),
                filterA,
                Assignments.of(
                        subqueryAProjection1, expression("subquery_a_column1 * 10"),
                        subqueryAColumn1, expression("subquery_a_column1")));

        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);
        Symbol subqueryBProjection1 = symbolAllocator.newSymbol("subquery_b_projection1", BIGINT);
        // subquery B scans just column 1
        PlanNode scanB = new TableScanNode(
                new PlanNodeId("scanB"),
                testTableHandle,
                ImmutableList.of(subqueryBColumn1),
                ImmutableMap.of(subqueryBColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
        // Subquery B predicate is subset of subquery A predicate. Subquery B has dynamic filter
        FilterNode filterB = new FilterNode(
                new PlanNodeId("filterB"),
                scanB,
                and(
                        expression("subquery_b_column1 > BIGINT '42'"),
                        createDynamicFilterExpression(
                                TEST_SESSION,
                                getQueryRunner().getMetadata(),
                                new DynamicFilterId("subquery_b_dynamic_id"),
                                BIGINT,
                                expression("subquery_b_column1"))));
        // Subquery B projection is subset of subquery 1 projection
        ProjectNode projectB = new ProjectNode(
                new PlanNodeId("projectB"),
                filterB,
                Assignments.of(
                        subqueryBProjection1, expression("subquery_b_column1 * 10")));

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(
                idAllocator,
                symbolAllocator,
                new UnionNode(
                        new PlanNodeId("union"),
                        ImmutableList.of(projectA, projectB),
                        ImmutableListMultimap.of(),
                        ImmutableList.of()));

        // there should be a common subquery found for both subplans
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).containsKey(projectA);
        assertThat(planAdaptations).containsKey(projectB);

        CommonPlanAdaptation subqueryA = planAdaptations.get(projectA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(projectB);

        // common subplan should be identical for both subqueries
        PlanMatchPattern commonSubplan = project(
                ImmutableMap.of(
                        "column1", PlanMatchPattern.expression("column1"),
                        "projection", PlanMatchPattern.expression("column1 * 10")),
                filter(
                        expression("column1 > BIGINT '42' OR column2 % 2 = BIGINT '0'"),
                        tableScan(
                                TEST_TABLE,
                                ImmutableMap.of(
                                        "column1", "column1",
                                        "column2", "column2"))));
        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);

        // assert that useConnectorNodePartitioning is propagated correctly
        assertThat(((TableScanNode) PlanNodeSearcher.searchFrom(subqueryA.getCommonSubplan())
                .whereIsInstanceOfAny(TableScanNode.class)
                .findOnlyElement())
                .isUseConnectorNodePartitioning())
                .isTrue();
        assertThat(((TableScanNode) PlanNodeSearcher.searchFrom(subqueryB.getCommonSubplan())
                .whereIsInstanceOfAny(TableScanNode.class)
                .findOnlyElement())
                .isUseConnectorNodePartitioning())
                .isFalse();

        // assert that common subplan for subquery A doesn't have dynamic filter
        assertThat(ExpressionExtractor.extractExpressions(subqueryA.getCommonSubplan()).stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream()))
                .isEmpty();

        // assert that common subplan for subquery B has dynamic filter preserved
        assertThat(ExpressionExtractor.extractExpressions(subqueryB.getCommonSubplan()).stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .collect(toImmutableList()))
                .containsExactly(new DynamicFilters.Descriptor(
                        new DynamicFilterId("subquery_b_dynamic_id"),
                        expression("subquery_b_column1")));

        // symbols used in common subplans for both subqueries should be unique
        assertThat(SymbolsExtractor.extractUnique(subqueryA.getCommonSubplan()))
                .doesNotContainAnyElementsOf(SymbolsExtractor.extractUnique(subqueryB.getCommonSubplan()));

        // since subqueryA has the same predicate and projections as common subquery, then no adaptation is required
        PlanNode subqueryACommonSubplan = subqueryA.getCommonSubplan();
        assertThat(subqueryA.adaptCommonSubplan(subqueryACommonSubplan, idAllocator)).isEqualTo(subqueryACommonSubplan);

        assertThat(subqueryB.getAdaptationPredicate()).contains(expression("subquery_b_column1 > BIGINT '42'"));
        assertThat(subqueryB.getAdaptationAssignments()).isPresent();
        assertThat(subqueryB.getAdaptationAssignments().get().getMap()).contains(
                new SimpleEntry<>(subqueryBProjection1, expression("subquery_b_column1 * 10")));

        // make sure plan signatures are same
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(subqueryB.getCommonSubplanSignature());
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(new PlanSignature(
                new SignatureKey("cache_table_id:((\"cache_column1\" > BIGINT '42') OR ((\"cache_column2\" % 2) = BIGINT '0'))"),
                Optional.empty(),
                ImmutableList.of(new CacheColumnId("(\"cache_column1\" * 10)"), new CacheColumnId("cache_column1")),
                TupleDomain.all()));
    }

    @Test
    public void testExtractDomain()
    {
        // both subqueries contain simple predicate that can be translated into tuple domain in plan signature
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);
        PlanNode scanA = new TableScanNode(
                new PlanNodeId("scanA"),
                testTableHandle,
                ImmutableList.of(subqueryAColumn1),
                ImmutableMap.of(subqueryAColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(true));
        FilterNode filterA = new FilterNode(
                new PlanNodeId("filterA"),
                scanA,
                expression("subquery_a_column1 > BIGINT '42'"));

        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);
        PlanNode scanB = new TableScanNode(
                new PlanNodeId("scanB"),
                testTableHandle,
                ImmutableList.of(subqueryBColumn1),
                ImmutableMap.of(subqueryBColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(true));
        FilterNode filterB = new FilterNode(
                new PlanNodeId("filterB"),
                scanB,
                expression("subquery_b_column1 < BIGINT '0'"));

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(
                idAllocator,
                symbolAllocator,
                new UnionNode(
                        new PlanNodeId("union"),
                        ImmutableList.of(filterA, filterB),
                        ImmutableListMultimap.of(),
                        ImmutableList.of()));

        // there should be a common subquery found for both subplans
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).containsKey(filterA);
        assertThat(planAdaptations).containsKey(filterB);

        CommonPlanAdaptation subqueryA = planAdaptations.get(filterA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(filterB);

        // common subplan should be identical for both subqueries
        PlanMatchPattern commonSubplan =
                filter(
                        expression("column1 > BIGINT '42' OR column1 < BIGINT '0'"),
                        tableScan(
                                TEST_TABLE,
                                ImmutableMap.of(
                                        "column1", "column1")));
        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);

        // filtering adaptation is required
        assertThat(subqueryA.getAdaptationPredicate()).contains(expression("subquery_a_column1 > BIGINT '42'"));
        assertThat(subqueryA.getAdaptationAssignments()).isEmpty();

        assertThat(subqueryB.getAdaptationPredicate()).contains(expression("subquery_b_column1 < BIGINT '0'"));
        assertThat(subqueryB.getAdaptationAssignments()).isEmpty();

        // make sure plan signatures are same and contain domain
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(subqueryB.getCommonSubplanSignature());
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(new PlanSignature(
                new SignatureKey("cache_table_id"),
                Optional.empty(),
                ImmutableList.of(new CacheColumnId("cache_column1")),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new CacheColumnId("cache_column1"), Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 0L), greaterThan(BIGINT, 42L)), false)))));
    }

    @Test
    public void testSimpleSubqueries()
    {
        // both subqueries just select single column
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol subqueryAColumn1 = symbolAllocator.newSymbol("subquery_a_column1", BIGINT);
        PlanNode scanA = new TableScanNode(
                new PlanNodeId("scanA"),
                testTableHandle,
                ImmutableList.of(subqueryAColumn1),
                ImmutableMap.of(subqueryAColumn1, HANDLE_1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(true));

        Symbol subqueryBColumn1 = symbolAllocator.newSymbol("subquery_b_column1", BIGINT);
        PlanNode scanB = new TableScanNode(
                new PlanNodeId("scanB"),
                testTableHandle,
                ImmutableList.of(subqueryBColumn1),
                ImmutableMap.of(subqueryBColumn1, HANDLE_2),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(true));

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Map<PlanNode, CommonPlanAdaptation> planAdaptations = extractCommonSubqueries(
                idAllocator,
                symbolAllocator,
                new UnionNode(
                        new PlanNodeId("union"),
                        ImmutableList.of(scanA, scanB),
                        ImmutableListMultimap.of(),
                        ImmutableList.of()));

        // there should be a common subquery found for both subplans
        assertThat(planAdaptations).hasSize(2);
        assertThat(planAdaptations).containsKey(scanA);
        assertThat(planAdaptations).containsKey(scanB);

        CommonPlanAdaptation subqueryA = planAdaptations.get(scanA);
        CommonPlanAdaptation subqueryB = planAdaptations.get(scanB);

        // common subplan should be identical for both subqueries
        PlanMatchPattern commonSubplan =
                tableScan(
                        TEST_TABLE,
                        ImmutableMap.of(
                                "column1", "column1",
                                "column2", "column2"));
        assertPlan(symbolAllocator, subqueryA.getCommonSubplan(), commonSubplan);
        assertPlan(symbolAllocator, subqueryB.getCommonSubplan(), commonSubplan);

        // only projection adaptation is required
        assertThat(subqueryA.getAdaptationPredicate()).isEmpty();
        assertThat(subqueryA.getAdaptationAssignments()).isPresent();
        assertThat(subqueryA.getAdaptationAssignments().get().getMap()).contains(
                new SimpleEntry<>(subqueryAColumn1, expression("subquery_a_column1")));

        assertThat(subqueryB.getAdaptationPredicate()).isEmpty();
        assertThat(subqueryB.getAdaptationAssignments()).isPresent();
        assertThat(subqueryB.getAdaptationAssignments().get().getMap()).contains(
                new SimpleEntry<>(subqueryBColumn1, expression("subquery_b_column1")));

        // make sure plan signatures are same and contain domain
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(subqueryB.getCommonSubplanSignature());
        assertThat(subqueryA.getCommonSubplanSignature()).isEqualTo(new PlanSignature(
                new SignatureKey("cache_table_id"),
                Optional.empty(),
                ImmutableList.of(new CacheColumnId("cache_column1"), new CacheColumnId("cache_column2")),
                TupleDomain.all()));
    }

    private Map<PlanNode, CommonPlanAdaptation> extractCommonSubqueries(
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            PlanNode root)
    {
        return getQueryRunner().inTransaction(TEST_SESSION, session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> getQueryRunner().getMetadata().getCatalogHandle(session, catalog));
            return CommonSubqueriesExtractor.extractCommonSubqueries(
                    getQueryRunner().getPlannerContext(),
                    session,
                    idAllocator,
                    symbolAllocator,
                    root);
        });
    }

    private void assertPlan(SymbolAllocator symbolAllocator, PlanNode root, PlanMatchPattern expected)
    {
        getQueryRunner().inTransaction(TEST_SESSION, session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> getQueryRunner().getMetadata().getCatalogHandle(session, catalog));
            Plan plan = new Plan(root, symbolAllocator.getTypes(), StatsAndCosts.empty());
            PlanAssert.assertPlan(session, getQueryRunner().getMetadata(), createTestingFunctionManager(), noopStatsCalculator(), plan, expected);
            return null;
        });
    }
}
