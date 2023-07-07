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

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingHandles;
import io.trino.testing.TestingMetadata;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.cache.CanonicalSubplanExtractor.extractCanonicalSubplans;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCanonicalizedSubplanExtractor
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private static final CacheTableId CACHE_TABLE_ID = new CacheTableId("cache_table_id");
    private static final Metadata TEST_METADATA = new TestMetadata();

    private PlanBuilder planBuilder;

    @BeforeClass
    public void setup()
    {
        planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), TEST_METADATA, TEST_SESSION);
    }

    @Test
    public void testExtractCanonicalScanAndProject()
    {
        ProjectNode projectNode = createScanAndProjectNode();
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                TEST_METADATA,
                TEST_SESSION,
                projectNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(projectNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("cache_column1"), new Symbol("symbol1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new Symbol("symbol2")),
                new SimpleEntry<>(new CacheColumnId("(\"cache_column1\" + 1)"), new Symbol("projection1")));
        assertThat(subplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("(\"cache_column1\" + 1)"), expression("cache_column1 + 1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new SymbolReference("cache_column2")));

        assertThat(subplan.getConjuncts()).isEmpty();
        assertThat(subplan.getDynamicConjuncts()).isEmpty();
        assertThat(subplan.getColumnHandles()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("cache_column1"), new TestingColumnHandle("column1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new TestingColumnHandle("column2")));
        assertThat(subplan.getTableId()).isEqualTo(CACHE_TABLE_ID);
        assertThat(subplan.getTableScanId()).isEqualTo(new PlanNodeId("scan_id"));
        assertThat(subplan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
    }

    @Test
    public void testExtractCanonicalFilterAndProject()
    {
        ProjectNode projectNode = createFilterAndProjectNode();
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                TEST_METADATA,
                TEST_SESSION,
                projectNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(projectNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("cache_column1"), new Symbol("symbol1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new Symbol("symbol2")),
                new SimpleEntry<>(new CacheColumnId("(\"cache_column1\" + 1)"), new Symbol("projection1")));
        assertThat(subplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("(\"cache_column1\" + 1)"), expression("cache_column1 + 1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new SymbolReference("cache_column2")));

        assertThat(subplan.getConjuncts()).hasSize(1);
        Expression predicate = getOnlyElement(subplan.getConjuncts());
        assertThat(predicate).isEqualTo(expression("cache_column1 + cache_column2 > 0"));

        assertThat(subplan.getDynamicConjuncts()).hasSize(1);
        Expression dynamicFilterExpression = getOnlyElement(subplan.getDynamicConjuncts());
        assertThat(DynamicFilters.getDescriptor(dynamicFilterExpression)).contains(
                new DynamicFilters.Descriptor(new DynamicFilterId("dynamic_filter_id"), expression("cache_column1")));

        assertThat(subplan.getColumnHandles()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("cache_column1"), new TestingColumnHandle("column1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new TestingColumnHandle("column2")));
        assertThat(subplan.getTableId()).isEqualTo(CACHE_TABLE_ID);
        assertThat(subplan.getTableScanId()).isEqualTo(new PlanNodeId("scan_id"));
        assertThat(subplan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
    }

    @Test
    public void testExtractCanonicalFilter()
    {
        FilterNode filterNode = createFilterNode();
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                TEST_METADATA,
                TEST_SESSION,
                filterNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(filterNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("cache_column1"), new Symbol("symbol1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new Symbol("symbol2")));
        assertThat(subplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("cache_column1"), new SymbolReference("cache_column1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new SymbolReference("cache_column2")));

        assertThat(subplan.getConjuncts()).hasSize(1);
        Expression predicate = getOnlyElement(subplan.getConjuncts());
        assertThat(predicate).isEqualTo(expression("cache_column1 + cache_column2 > 0"));

        assertThat(subplan.getDynamicConjuncts()).hasSize(1);
        Expression dynamicFilterExpression = getOnlyElement(subplan.getDynamicConjuncts());
        assertThat(DynamicFilters.getDescriptor(dynamicFilterExpression)).contains(
                new DynamicFilters.Descriptor(new DynamicFilterId("dynamic_filter_id"), expression("cache_column1")));

        assertThat(subplan.getColumnHandles()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("cache_column1"), new TestingColumnHandle("column1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new TestingColumnHandle("column2")));
        assertThat(subplan.getTableId()).isEqualTo(CACHE_TABLE_ID);
        assertThat(subplan.getTableScanId()).isEqualTo(new PlanNodeId("scan_id"));
        assertThat(subplan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
    }

    @Test
    public void testExtractCanonicalTableScan()
    {
        // no cache id, therefore no canonical plan
        TableScanNode tableScanNode = createTableScan();
        assertThat(extractCanonicalSubplans(
                new TestMetadata(Optional.empty(), handle -> Optional.of(new CacheColumnId(handle.getName()))),
                TEST_SESSION,
                tableScanNode))
                .isEmpty();

        // no column id, therefore no canonical plan
        assertThat(extractCanonicalSubplans(
                new TestMetadata(Optional.of(CACHE_TABLE_ID), handle -> Optional.empty()),
                TEST_SESSION,
                tableScanNode))
                .isEmpty();

        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                TEST_METADATA,
                TEST_SESSION,
                tableScanNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(tableScanNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("cache_column1"), new Symbol("symbol1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new Symbol("symbol2")));
        assertThat(subplan.getAssignments()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("cache_column1"), new SymbolReference("cache_column1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new SymbolReference("cache_column2")));
        assertThat(subplan.getConjuncts()).isEmpty();
        assertThat(subplan.getDynamicConjuncts()).isEmpty();
        assertThat(subplan.getColumnHandles()).containsExactly(
                new SimpleEntry<>(new CacheColumnId("cache_column1"), new TestingColumnHandle("column1")),
                new SimpleEntry<>(new CacheColumnId("cache_column2"), new TestingColumnHandle("column2")));
        assertThat(subplan.getTableId()).isEqualTo(CACHE_TABLE_ID);
        assertThat(subplan.getTableScanId()).isEqualTo(new PlanNodeId("scan_id"));
        assertThat(subplan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
    }

    @Test
    public void testTableHandlesCanonization()
    {
        TableHandle tableHandle1 = TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "table1"));
        TableHandle tableHandle2 = TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "table2"));

        PlanNode root = planBuilder.union(ImmutableListMultimap.of(), ImmutableList.of(
                planBuilder.tableScan(tableHandle1, ImmutableList.of(), ImmutableMap.of(), Optional.of(true)),
                planBuilder.tableScan(tableHandle2, ImmutableList.of(), ImmutableMap.of(), Optional.of(true))));

        // TableHandles will be turned into common canonical version
        List<CacheTableId> tableIds = extractCanonicalSubplans(
                new TestMetadata(
                        handle -> Optional.of(new CacheColumnId(handle.getName())),
                        (tableHandle) -> TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "common")),
                        (tableHandle) -> Optional.of(new CacheTableId(tableHandle.getConnectorHandle().toString()))),
                TEST_SESSION,
                root).stream().map(CanonicalSubplan::getTableId).toList();
        assertThat(tableIds).isEqualTo(ImmutableList.of(new CacheTableId("schema.common"), new CacheTableId("schema.common")));

        // TableHandles will not be turned into common canonical version
        tableIds = extractCanonicalSubplans(new TestMetadata(
                handle -> Optional.of(new CacheColumnId(handle.getName())),
                (tableHandle) -> {
                    TestingMetadata.TestingTableHandle handle = (TestingMetadata.TestingTableHandle) tableHandle.getConnectorHandle();
                    if (handle.getTableName().getTableName().equals("table1")) {
                        return TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "common1"));
                    }
                    else {
                        return TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "common2"));
                    }
                },
                (tableHandle) -> Optional.of(new CacheTableId(tableHandle.getConnectorHandle().toString()))),
                TEST_SESSION,
                root).stream().map(CanonicalSubplan::getTableId).toList();
        assertThat(tableIds).isEqualTo(ImmutableList.of(new CacheTableId("schema.common1"), new CacheTableId("schema.common2")));
    }

    private ProjectNode createScanAndProjectNode()
    {
        return new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol("projection1"),
                        expression("symbol1 + 1"),
                        new Symbol("symbol2"),
                        expression("symbol2")));
    }

    private ProjectNode createFilterAndProjectNode()
    {
        return new ProjectNode(
                new PlanNodeId("project_node"),
                createFilterNode(),
                Assignments.of(
                        new Symbol("projection1"),
                        expression("symbol1 + 1"),
                        new Symbol("symbol2"),
                        expression("symbol2")));
    }

    private FilterNode createFilterNode()
    {
        MetadataManager metadataManager = createTestMetadataManager();
        return new FilterNode(
                new PlanNodeId("filter_node"),
                createTableScan(),
                and(
                        expression("symbol1 + symbol2 > 0"),
                        createDynamicFilterExpression(
                                TEST_SESSION,
                                metadataManager,
                                new DynamicFilterId("dynamic_filter_id"),
                                BIGINT,
                                expression("symbol1"))));
    }

    private TableScanNode createTableScan()
    {
        Symbol symbol1 = new Symbol("symbol1");
        Symbol symbol2 = new Symbol("symbol2");
        TestingColumnHandle handle1 = new TestingColumnHandle("column1");
        TestingColumnHandle handle2 = new TestingColumnHandle("column2");
        return new TableScanNode(
                new PlanNodeId("scan_id"),
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol1, symbol2),
                ImmutableMap.of(symbol1, handle1, symbol2, handle2),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(true));
    }

    private static class TestMetadata
            extends AbstractMockMetadata
    {
        private final Function<TableHandle, Optional<CacheTableId>> tableHandleCacheTableIdMapper;

        private final Function<TestingColumnHandle, Optional<CacheColumnId>> cacheColumnIdMapper;
        private final Function<TableHandle, TableHandle> canonicalizeTableHande;

        private TestMetadata()
        {
            this(handle -> Optional.of(new CacheColumnId("cache_" + handle.getName())), Functions.identity(), (any) -> Optional.of(CACHE_TABLE_ID));
        }

        private TestMetadata(
                Optional<CacheTableId> cacheTableId,
                Function<TestingColumnHandle, Optional<CacheColumnId>> cacheColumnIdMapper)
        {
            this(cacheColumnIdMapper, Function.identity(), (any) -> cacheTableId);
        }

        private TestMetadata(
                Function<TestingColumnHandle, Optional<CacheColumnId>> cacheColumnIdMapper,
                Function<TableHandle, TableHandle> canonicalizeTableHande,
                Function<TableHandle, Optional<CacheTableId>> tableHandleCacheTableIdMapper)
        {
            this.cacheColumnIdMapper = cacheColumnIdMapper;
            this.canonicalizeTableHande = canonicalizeTableHande;
            this.tableHandleCacheTableIdMapper = tableHandleCacheTableIdMapper;
        }

        @Override
        public Optional<CacheTableId> getCacheTableId(Session session, TableHandle tableHandle)
        {
            return tableHandleCacheTableIdMapper.apply(tableHandle);
        }

        @Override
        public Optional<CacheColumnId> getCacheColumnId(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return cacheColumnIdMapper.apply((TestingColumnHandle) columnHandle);
        }

        @Override
        public TableHandle getCanonicalTableHandle(Session session, TableHandle tableHandle)
        {
            return canonicalizeTableHande.apply(tableHandle);
        }
    }
}
