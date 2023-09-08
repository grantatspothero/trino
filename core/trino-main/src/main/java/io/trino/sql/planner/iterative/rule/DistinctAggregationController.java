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

import com.google.inject.Inject;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;

import static io.trino.SystemSessionProperties.distinctAggregationsStrategy;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.AUTOMATIC;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.MARK_DISTINCT;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.PRE_AGGREGATE;
import static io.trino.sql.planner.OptimizerConfig.DistinctAggregationsStrategy.SINGLE_STEP;
import static io.trino.sql.planner.iterative.rule.DistinctAggregationToGroupBy.canUsePreAggregate;
import static io.trino.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct.canUseMarkDistinct;
import static java.util.Objects.requireNonNull;

/**
 * Controls which implementation of distinct aggregation should be used for particular {@link AggregationNode}
 */
public class DistinctAggregationController
{
    private static final int MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = 8;
    private static final int PRE_AGGREGATE_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * 8;

    private final TaskCountEstimator taskCountEstimator;

    @Inject
    public DistinctAggregationController(TaskCountEstimator taskCountEstimator)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    public boolean shouldAddMarkDistinct(AggregationNode aggregationNode, Rule.Context context)
    {
        return chooseMarkDistinctStrategy(aggregationNode, context) == MARK_DISTINCT;
    }

    public boolean shouldUsePreAggregate(AggregationNode aggregationNode, Rule.Context context)
    {
        return chooseMarkDistinctStrategy(aggregationNode, context) == PRE_AGGREGATE;
    }

    private DistinctAggregationsStrategy chooseMarkDistinctStrategy(AggregationNode aggregationNode, Rule.Context context)
    {
        DistinctAggregationsStrategy distinctAggregationsStrategy = distinctAggregationsStrategy(context.getSession());
        if (distinctAggregationsStrategy != AUTOMATIC) {
            if (distinctAggregationsStrategy == MARK_DISTINCT && canUseMarkDistinct(aggregationNode)) {
                return MARK_DISTINCT;
            }
            if (distinctAggregationsStrategy == PRE_AGGREGATE && canUsePreAggregate(aggregationNode)) {
                return PRE_AGGREGATE;
            }
            // in case strategy is chosen by the session property, but we cannot use it, lets fallback to single-step
            return SINGLE_STEP;
        }
        double numberOfDistinctValues = getMinDistinctValueCountEstimate(aggregationNode, context);
        int maxNumberOfConcurrentThreadsForAggregation = getMaxNumberOfConcurrentThreadsForAggregation(context);

        // use single_step if it can be parallelized
        // small numberOfDistinctValues reduces the distinct aggregation parallelism, also because the partitioning may be skewed.
        // this makes query to underutilize the cluster CPU but also to possibly concentrate memory on few nodes.
        // single_step alternatives should increase the parallelism at a cost of CPU.
        if (!aggregationNode.getGroupingKeys().isEmpty() && // global distinct aggregation is computed using a single thread. Strategies other than single_step will help parallelize the execution.
                !Double.isNaN(numberOfDistinctValues) && // if the estimate is unknown, use alternatives to avoid query failure
                (numberOfDistinctValues > PRE_AGGREGATE_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * maxNumberOfConcurrentThreadsForAggregation ||
                        (numberOfDistinctValues > MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * maxNumberOfConcurrentThreadsForAggregation &&
                                // if the NDV and the number of grouping keys is small, pre-aggregate is faster than single_step at a cost of CPU
                                aggregationNode.getGroupingKeys().size() > 2))) {
            return SINGLE_STEP;
        }

        // mark-distinct is better than pre-aggregate if the number of group-by keys is bigger than 2
        // because group-by keys are added to every grouping set and this makes partial aggregation behaves badly
        if (canUsePreAggregate(aggregationNode) && aggregationNode.getGroupingKeys().size() <= 2) {
            return PRE_AGGREGATE;
        }
        else if (canUseMarkDistinct(aggregationNode)) {
            return MARK_DISTINCT;
        }

        // if no strategy found, use single_step by default
        return SINGLE_STEP;
    }

    private int getMaxNumberOfConcurrentThreadsForAggregation(Rule.Context context)
    {
        return taskCountEstimator.estimateHashedTaskCount(context.getSession()) * getTaskConcurrency(context.getSession());
    }

    private double getMinDistinctValueCountEstimate(AggregationNode aggregationNode, Rule.Context context)
    {
        // NDV stats for multiple grouping keys are unreliable, let's pick a conservative lower bound by taking maximum NDV for all grouping keys.
        // this assumes that grouping keys are 100% correlated.
        // in the case of a lower correlation, the NDV can only be higher.
        PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(aggregationNode.getSource());
        double max = Double.NaN;
        for (Symbol groupingKey : aggregationNode.getGroupingKeys()) {
            double distinctValuesCount = sourceStats.getSymbolStatistics(groupingKey).getDistinctValuesCount();
            if (Double.isNaN(max) || distinctValuesCount > max) {
                max = distinctValuesCount;
            }
        }
        return max;
    }
}
