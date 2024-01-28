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

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.QualifiedObjectName;
import io.trino.operator.OperatorStats;
import io.trino.server.DynamicFilterService;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;

public class TestHiveDynamicPartitionPruningTestWithPlanAlternatives
        extends TestHiveDynamicPartitionPruningTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setExtraProperties(EXTRA_PROPERTIES)
                .setHiveProperties(ImmutableMap.of("hive.dynamic-filtering.wait-timeout", "1h"))
                .setInitialTables(REQUIRED_TABLES)
                .withPlanAlternatives()
                .build();
    }

    @Test
    @Timeout(value = 30_000, unit = TimeUnit.MILLISECONDS)
    public void testJoinWithSelectiveBuildSideAndAlternativesOnBothSides()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey " +
                "AND supplier.name = 'Supplier#000000001' AND partitioned_lineitem.partkey > 880";
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(
                getSession(),
                selectQuery);
        MaterializedResult expected = computeActual(withDynamicFilteringDisabled(), selectQuery);
        assertEqualsIgnoreOrder(result.result(), expected);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(result.queryId(), getQualifiedTableName(PARTITIONED_LINEITEM));
        // Probe-side is partially scanned
        assertEquals(probeStats.getInputPositions(), 369L);

        DynamicFilterService.DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.queryId());
        assertEquals(dynamicFiltersStats.getTotalDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getLazyDynamicFilters(), 1L);
        assertEquals(dynamicFiltersStats.getReplicatedDynamicFilters(), 0L);
        assertEquals(dynamicFiltersStats.getDynamicFiltersCompleted(), 1L);

        DynamicFilterService.DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
        assertEquals(domainStats.getSimplifiedDomain(), singleValue(BIGINT, 1L).toString(getSession().toConnectorSession()));
    }

    @Override
    protected OperatorStats searchScanFilterAndProjectOperatorStats(QueryId queryId, QualifiedObjectName catalogSchemaTableName)
    {
        DistributedQueryRunner runner = getDistributedQueryRunner();
        Plan plan = runner.getQueryPlan(queryId);
        // we need to sum multiple table scan operators in case of sub-plan alternatives
        Set<PlanNodeId> nodeIds = PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(node -> {
                    // project -> filter -> scan can be split by ChooseAlternative. In that case there is no ScanFilterAndProjectOperator but rather TableScanOperator
                    PlanNode nodeToCheck = node;
                    if (nodeToCheck instanceof ProjectNode projectNode) {
                        nodeToCheck = projectNode.getSource();
                    }
                    if (nodeToCheck instanceof FilterNode filterNode) {
                        nodeToCheck = filterNode.getSource();
                    }
                    if (nodeToCheck instanceof TableScanNode tableScanNode) {
                        CatalogSchemaTableName tableName = getTableName(tableScanNode.getTable());
                        return tableName.equals(catalogSchemaTableName.asCatalogSchemaTableName());
                    }
                    return false;
                })
                .findAll()
                .stream()
                .map(PlanNode::getId)
                .collect(toImmutableSet());

        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> nodeIds.contains(summary.getPlanNodeId())
                        && (summary.getOperatorType().equals("ScanFilterAndProjectOperator") || summary.getOperatorType().equals("TableScanOperator")))
                .reduce(TestHiveDynamicRowFilteringWithPlanAlternatives::addOperatorStats)
                .orElseThrow(() -> new NoSuchElementException("table scan operator not found for " + catalogSchemaTableName + " in " + queryId + " nodeIds: " + nodeIds));
    }
}
