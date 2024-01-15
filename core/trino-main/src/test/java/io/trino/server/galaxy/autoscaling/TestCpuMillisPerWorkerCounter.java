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

import com.google.common.collect.ImmutableMap;
import io.airlift.stats.ExponentialDecay;
import io.airlift.testing.TestingTicker;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class TestCpuMillisPerWorkerCounter
{
    @Test
    void shouldOnlyAddDifference()
    {
        TestingTicker ticker = new TestingTicker();

        CpuMillisPerWorkerCounter cpuMillisPerWorkerCounter = new CpuMillisPerWorkerCounter(ExponentialDecay.oneMinute(), ticker);
        cpuMillisPerWorkerCounter.update(ImmutableMap.of(new QueryId("test_1"), 200L), 2);

        assertThat(cpuMillisPerWorkerCounter.getCount()).isEqualTo(200 / 2);

        // given the same total cpuMillis for query 1, the delta is 0 so cpuMillis should not change
        cpuMillisPerWorkerCounter.update(ImmutableMap.of(new QueryId("test_1"), 200L), 2);
        assertThat(cpuMillisPerWorkerCounter.getCount()).isEqualTo(200 / 2);

        cpuMillisPerWorkerCounter.update(ImmutableMap.of(new QueryId("test_1"), 500L), 2);
        int expected = (200 / 2) + (300 / 2);
        assertThat(cpuMillisPerWorkerCounter.getCount()).isEqualTo(expected);
    }

    @Test
    void shouldDecayCounter()
    {
        TestingTicker ticker = new TestingTicker();

        CpuMillisPerWorkerCounter counter = new CpuMillisPerWorkerCounter(ExponentialDecay.oneMinute(), ticker);
        counter.update(ImmutableMap.of(new QueryId("test_1"), 200L), 1);

        assertThat(counter.getCount()).isEqualTo(200);

        ticker.increment(1, TimeUnit.MINUTES);
        counter.update(ImmutableMap.of(new QueryId("test_1"), 500L), 1);

        // current delta should be counted in full, previous decays
        double expected = (500 - 200) + 200 / Math.E;
        assertThat(counter.getCount()).isEqualTo(expected, within(1e-9));
    }

    @Test
    void shouldAggregateQueryDifferences()
    {
        TestingTicker ticker = new TestingTicker();

        CpuMillisPerWorkerCounter cpuMillisPerWorkerCounter = new CpuMillisPerWorkerCounter(ExponentialDecay.oneMinute(), ticker);
        cpuMillisPerWorkerCounter.update(ImmutableMap.of(new QueryId("test_1"), 200L), 2);

        int firstStepExpected = 100;
        assertThat(cpuMillisPerWorkerCounter.getCount()).isEqualTo(firstStepExpected);

        cpuMillisPerWorkerCounter.update(ImmutableMap.of(
                        new QueryId("test_1"), 500L,
                        new QueryId("test_2"), 100L),
                2);

        double expected = firstStepExpected + ((500 - 200) + 100) / 2.0;
        assertThat(cpuMillisPerWorkerCounter.getCount()).isEqualTo(expected);
    }
}
