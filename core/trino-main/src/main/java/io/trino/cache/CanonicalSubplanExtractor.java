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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static java.util.Objects.requireNonNull;

public final class CanonicalSubplanExtractor
{
    private CanonicalSubplanExtractor() {}

    /**
     * Extracts a list of {@link CanonicalSubplan} for a given plan.
     */
    public static List<CanonicalSubplan> extractCanonicalSubplans(Metadata metadata, Session session, PlanNode root)
    {
        ImmutableList.Builder<CanonicalSubplan> canonicalSubplans = ImmutableList.builder();
        root.accept(new Visitor(metadata, session, canonicalSubplans), null).ifPresent(canonicalSubplans::add);
        return canonicalSubplans.build();
    }

    private static class Visitor
            extends PlanVisitor<Optional<CanonicalSubplan>, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final ImmutableList.Builder<CanonicalSubplan> canonicalSubplans;

        public Visitor(Metadata metadata, Session session, ImmutableList.Builder<CanonicalSubplan> canonicalSubplans)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.canonicalSubplans = requireNonNull(canonicalSubplans, "canonicalSubplans is null");
        }

        @Override
        protected Optional<CanonicalSubplan> visitPlan(PlanNode node, Void context)
        {
            node.getSources().forEach(this::canonicalizeRecursively);
            return Optional.empty();
        }

        @Override
        public Optional<CanonicalSubplan> visitProject(ProjectNode node, Void context)
        {
            if (!(node.getSource() instanceof FilterNode) && !(node.getSource() instanceof TableScanNode)) {
                // only scan <- filter <- project or scan <- project plans are supported
                canonicalizeRecursively(node.getSource());
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional = node.getSource().accept(this, null);
            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            CanonicalSubplan subplan = subplanOptional.get();
            // canonicalize projection assignments
            ImmutableMap.Builder<CacheColumnId, Expression> assignments = ImmutableMap.builder();
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.<CacheColumnId, Symbol>builder()
                    .putAll(subplan.getOriginalSymbolMapping());
            for (Map.Entry<Symbol, Expression> assignment : node.getAssignments().entrySet()) {
                Symbol symbol = assignment.getKey();
                if (node.getAssignments().isIdentity(symbol)) {
                    // canonicalize identity assignment
                    checkState(subplan.getOriginalSymbolMapping().inverse().containsKey(symbol));
                    CacheColumnId columnId = subplan.getOriginalSymbolMapping().inverse().get(symbol);
                    assignments.put(columnId, new SymbolReference(columnId.toString()));
                    continue;
                }

                // use formatted canonical expression as column id for non-identity projections
                Expression canonicalExpression = subplan.canonicalSymbolMapper().map(assignment.getValue());
                CacheColumnId columnId = new CacheColumnId(formatExpression(canonicalExpression));
                assignments.put(columnId, canonicalExpression);
                symbolMappingBuilder.put(columnId, symbol);
            }

            return Optional.of(new CanonicalSubplan(
                    node,
                    symbolMappingBuilder.build(),
                    assignments.buildOrThrow(),
                    subplan.getConjuncts(),
                    subplan.getDynamicConjuncts(),
                    subplan.getColumnHandles(),
                    subplan.getTable(),
                    subplan.getTableScanId(),
                    subplan.getTableId(),
                    subplan.isUseConnectorNodePartitioning()));
        }

        @Override
        public Optional<CanonicalSubplan> visitFilter(FilterNode node, Void context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                // only scan <- filter <- project or scan <- project plans are supported
                canonicalizeRecursively(node.getSource());
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional = node.getSource().accept(this, null);
            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            CanonicalSubplan subplan = subplanOptional.get();

            // extract dynamic and static conjuncts
            SymbolMapper canonicalSymbolMapper = subplan.canonicalSymbolMapper();
            ImmutableList.Builder<Expression> conjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> dynamicConjuncts = ImmutableList.builder();
            for (Expression expression : extractConjuncts(node.getPredicate())) {
                if (isDynamicFilter(expression)) {
                    dynamicConjuncts.add(canonicalSymbolMapper.map(expression));
                }
                else {
                    conjuncts.add(canonicalSymbolMapper.map(expression));
                }
            }

            return Optional.of(new CanonicalSubplan(
                    node,
                    subplan.getOriginalSymbolMapping(),
                    subplan.getAssignments(),
                    conjuncts.build(),
                    dynamicConjuncts.build(),
                    subplan.getColumnHandles(),
                    subplan.getTable(),
                    subplan.getTableScanId(),
                    subplan.getTableId(),
                    subplan.isUseConnectorNodePartitioning()));
        }

        @Override
        public Optional<CanonicalSubplan> visitTableScan(TableScanNode node, Void context)
        {
            if (node.isUpdateTarget()) {
                // inserts are not supported
                return Optional.empty();
            }

            Optional<CacheTableId> tableId = metadata.getCacheTableId(session, node.getTable());
            if (tableId.isEmpty()) {
                return Optional.empty();
            }

            // canonicalize output symbols using column ids
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.builder();
            ImmutableMap.Builder<CacheColumnId, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
            for (Map.Entry<Symbol, ColumnHandle> assignment : node.getAssignments().entrySet()) {
                Optional<CacheColumnId> columnId = metadata.getCacheColumnId(
                        session,
                        node.getTable(),
                        assignment.getValue());
                if (columnId.isEmpty()) {
                    return Optional.empty();
                }
                symbolMappingBuilder.put(columnId.get(), assignment.getKey());
                columnHandlesBuilder.put(columnId.get(), assignment.getValue());
            }
            BiMap<CacheColumnId, Symbol> symbolMapping = symbolMappingBuilder.build();

            // pass-through canonical output symbols
            Map<CacheColumnId, Expression> assignments = symbolMapping.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> new SymbolReference(entry.getKey().toString())));

            return Optional.of(new CanonicalSubplan(
                    node,
                    symbolMapping,
                    assignments,
                    // No filters in table scan. Relevant pushed predicates are
                    // part of CacheTableId, so such subplans won't be considered
                    // as similar.
                    ImmutableList.of(),
                    // no dynamic filters in table scan
                    ImmutableList.of(),
                    columnHandlesBuilder.buildOrThrow(),
                    node.getTable(),
                    node.getId(),
                    tableId.get(),
                    node.isUseConnectorNodePartitioning()));
        }

        private void canonicalizeRecursively(PlanNode node)
        {
            node.accept(this, null).ifPresent(canonicalSubplans::add);
        }
    }
}
