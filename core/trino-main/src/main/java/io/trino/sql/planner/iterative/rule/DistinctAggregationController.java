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
import io.trino.cost.TaskCountEstimator;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;

import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.isOptimizeDistinctAggregationEnabled;
import static java.util.Objects.requireNonNull;

/**
 * Controls which implementation of distinct aggregation should be used for particular {@link AggregationNode}
 */
public class DistinctAggregationController
{
    private static final int MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = 8;
    private static final int OPTIMIZED_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * 8;

    private final TaskCountEstimator taskCountEstimator;

    @Inject
    public DistinctAggregationController(TaskCountEstimator taskCountEstimator)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    public boolean shouldAddMarkDistinct(AggregationNode aggregationNode, Rule.Context context)
    {
        if (aggregationNode.getGroupingKeys().isEmpty()) {
            // global distinct aggregation is computed using a single thread. MarkDistinct will help parallelize the execution.
            return true;
        }
        if (aggregationNode.getGroupingKeys().size() > 1) {
            // NDV stats for multiple grouping keys are unreliable, let's keep MarkDistinct for this case to avoid significant slowdown or OOM/too big hash table issues in case of
            // overestimation of very small NDV with big number of distinct values inside the groups.
            return true;
        }
        double numberOfDistinctValues = context.getStatsProvider().getStats(aggregationNode).getOutputRowCount();
        if (Double.isNaN(numberOfDistinctValues)) {
            // if the estimate is unknown, use MarkDistinct to avoid query failure
            return true;
        }
        int maxNumberOfConcurrentThreadsForAggregation = getMaxNumberOfConcurrentThreadsForAggregation(context);

        if (numberOfDistinctValues <= MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * maxNumberOfConcurrentThreadsForAggregation) {
            // small numberOfDistinctValues reduces the distinct aggregation parallelism, also because the partitioning may be skewed.
            // This makes query to underutilize the cluster CPU but also to possibly concentrate memory on few nodes.
            // MarkDistinct should increase the parallelism at a cost of CPU.
            return true;
        }

        if (isOptimizeDistinctAggregationEnabled(context.getSession()) &&
                numberOfDistinctValues <= OPTIMIZED_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * maxNumberOfConcurrentThreadsForAggregation &&
                hasSingleDistinctAndNonDistincts(aggregationNode)) {
            // with medium number of numberOfDistinctValues, OptimizeMixedDistinctAggregations
            // will be beneficial for query latency (duration) over distinct aggregation at a cost of increased CPU,
            // but it relies on the existence of MarkDistinct nodes.
            return true;
        }

        // can parallelize single-step, and single-step distinct is more efficient than MarkDistinct
        return false;
    }

    private int getMaxNumberOfConcurrentThreadsForAggregation(Rule.Context context)
    {
        return taskCountEstimator.estimateHashedTaskCount(context.getSession()) * getTaskConcurrency(context.getSession());
    }

    private static boolean hasSingleDistinctAndNonDistincts(AggregationNode aggregationNode)
    {
        long distincts = aggregationNode.getAggregations()
                .values().stream()
                .filter(AggregationNode.Aggregation::isDistinct)
                .count();

        return distincts == 1 && distincts < aggregationNode.getAggregations().size();
    }
}
