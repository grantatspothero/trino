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
package io.trino.client;

import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestDrainState
{
    private static final JsonCodec<DrainState> DRAIN_STATE_JSON_CODEC = jsonCodec(DrainState.class);

    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(new DrainState(DrainState.Type.UNDRAINED, Optional.empty()));
        assertJsonRoundTrip(new DrainState(DrainState.Type.DRAINING, Optional.of(new Duration(30, TimeUnit.SECONDS))));
        assertJsonRoundTrip(new DrainState(DrainState.Type.DRAINED, Optional.empty()));
    }

    private static void assertJsonRoundTrip(DrainState drainState)
    {
        String json = DRAIN_STATE_JSON_CODEC.toJson(drainState);
        DrainState copy = DRAIN_STATE_JSON_CODEC.fromJson(json);
        assertEquals(copy, drainState);
    }
}
