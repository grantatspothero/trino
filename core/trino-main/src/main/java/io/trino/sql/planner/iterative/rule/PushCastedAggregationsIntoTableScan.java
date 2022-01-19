package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionId;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.iterative.rule.Rules.deriveTableStatisticsForPushdown;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class PushCastedAggregationsIntoTableScan implements Rule<AggregationNode> {
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Capture<ProjectNode> PROJECT = newCapture();

    private static final Pattern<AggregationNode> PATTERN =
            aggregation()
                    .with(step().equalTo(AggregationNode.Step.SINGLE))
                    .matching(PushCastedAggregationsIntoTableScan::allArgumentsAreSimpleReferences)
                    // skip arguments that are, for instance, lambda expressions
                    .matching(node -> node.getGroupingSets().getGroupingSetCount() <= 1)
                    .with(
                            source().matching(project().matching(PushCastedAggregationsIntoTableScan::allArgumentsAreSimpleReferencesOrCasts).capturedAs(PROJECT).with(
                                    source().matching(tableScan().capturedAs(TABLE_SCAN)))));

    private final PlannerContext plannerContext;

    public PushCastedAggregationsIntoTableScan(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    private static boolean allArgumentsAreSimpleReferences(AggregationNode node)
    {
        return node.getAggregations()
                .values().stream()
                .flatMap(aggregation -> aggregation.getArguments().stream())
                .allMatch(SymbolReference.class::isInstance);
    }

    private static boolean allArgumentsAreSimpleReferencesOrCasts(ProjectNode node)
    {
        return node.getAssignments().getExpressions().stream()
                .allMatch(expression -> expression instanceof SymbolReference || expression instanceof Cast);
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT);
        TableScanNode tableScanNode = captures.get(TABLE_SCAN);
        // TODO: verify no casting funny business is happening in the group by of the aggregation

        Map<Symbol, AggregationNode.Aggregation> rewrittenAggregations = node.getAggregations().entrySet().stream().map(
                entry -> {
                    ResolvedFunction rewrittenFunction = entry.getValue().getResolvedFunction();
//                    BoundSignature oldSignature = entry.getValue().getResolvedFunction().getSignature();
//                    BoundSignature rewrittenSignature = new BoundSignature(oldSignature.getName(), )
//                    FunctionId.toFunctionId()
                    List<Expression> rewrittenArguments = entry.getValue().getArguments().stream().map(expression -> {
                        Expression projectExpression = projectNode.getAssignments().get(Symbol.from(expression));
                        if(projectExpression instanceof Cast){
                            return projectExpression;
                        }
                        // No rewriting needed
                        return expression;
                    }).collect(Collectors.toList());
                    AggregationNode.Aggregation rewrittenAggregation  = new AggregationNode.Aggregation(rewrittenFunction, rewrittenArguments, entry.getValue().isDistinct(), entry.getValue().getFilter(), entry.getValue().getOrderingScheme(), entry.getValue().getMask());
                    return Map.entry(entry.getKey(), rewrittenAggregation);
                }
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        AggregationNode rewrittenAggregationNode = new AggregationNode(node.getId(), tableScanNode, rewrittenAggregations, node.getGroupingSets(), node.getPreGroupedSymbols(), node.getStep(), node.getHashSymbol(), node.getGroupIdSymbol());

        return pushAggregationIntoTableScan(plannerContext, context, rewrittenAggregationNode, projectNode, tableScanNode, rewrittenAggregations, rewrittenAggregationNode.getGroupingKeys())
                .map(Rule.Result::ofPlanNode)
                .orElseGet(Rule.Result::empty);
    }

    public static Optional<PlanNode> pushAggregationIntoTableScan(
            PlannerContext plannerContext,
            Context context,
            PlanNode aggregationNode,
            ProjectNode projectNode,
            TableScanNode tableScan,
            Map<Symbol, AggregationNode.Aggregation> aggregations,
            List<Symbol> groupingKeys)
    {
        Map<String, ColumnHandle> assignments = tableScan.getAssignments()
                .entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

        List<Map.Entry<Symbol, AggregationNode.Aggregation>> aggregationsList = aggregations
                .entrySet().stream()
                .collect(toImmutableList());

        List<AggregateFunction> aggregateFunctions = aggregationsList.stream()
                .map(Map.Entry::getValue)
                .map(aggregation -> PushAggregationIntoTableScan.toAggregateFunction(plannerContext.getMetadata(), context, aggregation))
                .collect(toImmutableList());

        List<Symbol> aggregationOutputSymbols = aggregationsList.stream()
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        List<ColumnHandle> groupByColumns = groupingKeys.stream()
                .map(groupByColumn -> assignments.get(groupByColumn.getName()))
                .collect(toImmutableList());

        Optional<AggregationApplicationResult<TableHandle>> aggregationPushdownResult = plannerContext.getMetadata().applyAggregation(
                context.getSession(),
                tableScan.getTable(),
                aggregateFunctions,
                assignments,
                ImmutableList.of(groupByColumns));

        if (aggregationPushdownResult.isEmpty()) {
            return Optional.empty();
        }

        AggregationApplicationResult<TableHandle> result = aggregationPushdownResult.get();

        // The new scan outputs should be the symbols associated with grouping columns plus the symbols associated with aggregations.
        ImmutableList.Builder<Symbol> newScanOutputs = new ImmutableList.Builder<>();
        newScanOutputs.addAll(tableScan.getOutputSymbols());

        ImmutableBiMap.Builder<Symbol, ColumnHandle> newScanAssignments = new ImmutableBiMap.Builder<>();
        newScanAssignments.putAll(tableScan.getAssignments());

        Map<String, Symbol> variableMappings = new HashMap<>();

        for (Assignment assignment : result.getAssignments()) {
            Symbol symbol = context.getSymbolAllocator().newSymbol(assignment.getVariable(), assignment.getType());

            newScanOutputs.add(symbol);
            newScanAssignments.put(symbol, assignment.getColumn());
            variableMappings.put(assignment.getVariable(), symbol);
        }

        List<Expression> newProjections = result.getProjections().stream()
                .map(expression -> ConnectorExpressionTranslator.translate(context.getSession(), expression, variableMappings, new LiteralEncoder(plannerContext)))
                .collect(toImmutableList());

        verify(aggregationOutputSymbols.size() == newProjections.size());

        Assignments.Builder assignmentBuilder = Assignments.builder();
        IntStream.range(0, aggregationOutputSymbols.size())
                .forEach(index -> assignmentBuilder.put(aggregationOutputSymbols.get(index), newProjections.get(index)));

        ImmutableBiMap<Symbol, ColumnHandle> scanAssignments = newScanAssignments.build();
        ImmutableBiMap<ColumnHandle, Symbol> columnHandleToSymbol = scanAssignments.inverse();
        // projections assignmentBuilder should have both agg and group by so we add all the group bys as symbol references
        groupingKeys
                .forEach(groupBySymbol -> {
                    // if the connector returned a new mapping from oldColumnHandle to newColumnHandle, groupBy needs to point to
                    // new columnHandle's symbol reference, otherwise it will continue pointing at oldColumnHandle.
                    ColumnHandle originalColumnHandle = assignments.get(groupBySymbol.getName());
                    ColumnHandle groupByColumnHandle = result.getGroupingColumnMapping().getOrDefault(originalColumnHandle, originalColumnHandle);
                    assignmentBuilder.put(groupBySymbol, columnHandleToSymbol.get(groupByColumnHandle).toSymbolReference());
                });

        return Optional.of(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new TableScanNode(
                                context.getIdAllocator().getNextId(),
                                result.getHandle(),
                                newScanOutputs.build(),
                                scanAssignments,
                                TupleDomain.all(),
                                deriveTableStatisticsForPushdown(context.getStatsProvider(), context.getSession(), result.isPrecalculateStatistics(), aggregationNode),
                                tableScan.isUpdateTarget(),
                                // table scan partitioning might have changed with new table handle
                                Optional.empty()),
                        assignmentBuilder.build()));
    }


}
