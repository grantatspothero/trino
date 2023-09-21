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
package io.trino.server;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.dispatcher.DispatchManager;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class WorkerRecommendationProvider
{
    private final DispatchManager dispatchManager;
    private final int targetQueriesPerNode;

    @Inject
    public WorkerRecommendationProvider(GalaxyTrinoAutoscalingConfig config, DispatchManager dispatchManager)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.targetQueriesPerNode = requireNonNull(config, "config is null").getTargetQueriesPerNode();
    }

    // Current implementation might be good for streaming
    // The FTE can gain some more by utilizing information available in the following.
    // a: look into BinPackingNodeAllocatorService, are there tasks waiting for node, look at schedulers per query...
    // b: EventDrivenFaultTolerantQueryScheduler, SchedulingQueue - tasks are waiting for scheduling
    WorkerRecommendation get()
    {
        Map<String, String> metrics = ImmutableMap.<String, String>builder()
                .put("SubmittedQueries", Long.toString(dispatchManager.getStats().getSubmittedQueries().getTotalCount()))
                .put("CompletedQueries", Long.toString(dispatchManager.getStats().getCompletedQueries().getTotalCount()))
                .buildOrThrow();

        long submittedQueries = dispatchManager.getStats().getSubmittedQueries().getTotalCount();
        long completedQueries = dispatchManager.getStats().getCompletedQueries().getTotalCount();
        long totalQueriesCount = submittedQueries - completedQueries;

        int recommendedNodes = (int) (totalQueriesCount / targetQueriesPerNode);

        return new WorkerRecommendation(recommendedNodes, metrics);
    }
}
