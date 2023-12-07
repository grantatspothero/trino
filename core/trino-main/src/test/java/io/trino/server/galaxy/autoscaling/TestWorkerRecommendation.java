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
package io.trino.server.galaxy.autoscaling;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestWorkerRecommendation
{
    private static final JsonCodec<WorkerRecommendation> WORKER_RECOMMENDATION_JSON_CODEC = jsonCodec(WorkerRecommendation.class);

    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(new WorkerRecommendation(1, Collections.emptyMap()));
        assertJsonRoundTrip(new WorkerRecommendation(2, Collections.emptyMap()));
        assertJsonRoundTrip(new WorkerRecommendation(3, Map.of("queueSize", "123")));
    }

    private static void assertJsonRoundTrip(WorkerRecommendation workers)
    {
        String json = WORKER_RECOMMENDATION_JSON_CODEC.toJson(workers);
        WorkerRecommendation copy = WORKER_RECOMMENDATION_JSON_CODEC.fromJson(json);
        assertEquals(copy, workers);
    }
}
