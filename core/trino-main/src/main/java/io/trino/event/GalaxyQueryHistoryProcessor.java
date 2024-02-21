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
package io.trino.event;

import com.google.common.collect.ImmutableList;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.spi.eventlistener.StageDetails;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Ported from Galaxy QueryHistoryPayloadProcessor, pre-process stage info to reduce Kafka payload size.
 */
public final class GalaxyQueryHistoryProcessor
{
    private GalaxyQueryHistoryProcessor() {}

    public static List<StageDetails> processStageDetails(StageInfo stageInfo)
    {
        return getAllStages(stageInfo).stream()
                .map(GalaxyQueryHistoryProcessor::toStageDetails)
                .collect(toImmutableList());
    }

    private static List<StageInfo> getAllStages(StageInfo stageInfo)
    {
        ImmutableList.Builder<StageInfo> builder = ImmutableList.builder();
        addAllStages(stageInfo, builder);
        return builder.build();
    }

    private static void addAllStages(StageInfo stage, ImmutableList.Builder<StageInfo> builder)
    {
        builder.add(stage);
        for (StageInfo subStage : stage.getSubStages()) {
            addAllStages(subStage, builder);
        }
    }

    private static StageDetails toStageDetails(StageInfo stageInfo)
    {
        String stageId = stageInfo.getStageId().toString();
        return new StageDetails(
                stageId.substring(stageId.indexOf('.') + 1),
                stageInfo.getState().toString(),
                stageInfo.getTasks().size(),
                stageInfo.getStageStats().isFullyBlocked(),
                stageInfo.getStageStats().getTotalScheduledTime().toMillis(),
                stageInfo.getStageStats().getTotalBlockedTime().toMillis(),
                stageInfo.getStageStats().getTotalCpuTime().toMillis(),
                getStageInputBuffer(stageInfo));
    }

    private static long getStageInputBuffer(StageInfo stage)
    {
        // stage input buffer = sum of task output buffers across all of immediate sub-stages
        long buffer = 0;
        for (StageInfo subStage : stage.getSubStages()) {
            for (TaskInfo task : subStage.getTasks()) {
                buffer += task.getOutputBuffers().getTotalBufferedBytes();
            }
        }
        return buffer;
    }
}
