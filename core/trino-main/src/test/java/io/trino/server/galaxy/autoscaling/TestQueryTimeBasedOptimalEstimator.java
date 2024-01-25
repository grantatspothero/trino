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

import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TestQueryTimeBasedOptimalEstimator
{
    public static final int NODE_STARTUP_TIME = 60_000;
    public static final int TARGET_LATENCY = 10_000;

    @Test
    void shouldRescale()
    {
        int actualSize = 0;
        QueryTimeBasedOptimalEstimatorConfig config = new QueryTimeBasedOptimalEstimatorConfig();
        config.setNodeStartupTime(new Duration(NODE_STARTUP_TIME, TimeUnit.MILLISECONDS));
        config.setTargetLatency(new Duration(TARGET_LATENCY, TimeUnit.MILLISECONDS));
        config.setRemainingTimeScaleUpThreshold(new Duration(1, TimeUnit.SECONDS));
        config.setRemainingTimeScaleDownThreshold(new Duration(10, TimeUnit.SECONDS));

        QueryTimeBasedOptimalEstimator queryTimeBasedOptimalEstimator = new QueryTimeBasedOptimalEstimator(config);

        // scaleDown - time less than RemainingTimeScaleDownThreshold
        actualSize = queryTimeBasedOptimalEstimator.estimate(1, 1, 10);
        assertThat(actualSize).isEqualTo(8);
        actualSize = queryTimeBasedOptimalEstimator.estimate(5_000 * 10, 1, 10);
        assertThat(actualSize).isEqualTo(8);
        actualSize = queryTimeBasedOptimalEstimator.estimate(20_000 * 10, 3, 10);
        assertThat(actualSize).isEqualTo(8);

        // do nothing - time bigger than RemainingTimeScaleDownThreshold, but not enough to scaleup
        // do nothing min
        actualSize = queryTimeBasedOptimalEstimator.estimate(11_000 * 10, 1, 10);
        assertThat(actualSize).isEqualTo(10);
        actualSize = queryTimeBasedOptimalEstimator.estimate(11_000 * 20, 2, 10);
        assertThat(actualSize).isEqualTo(10);

        // do nothing max
        actualSize = queryTimeBasedOptimalEstimator.estimate(NODE_STARTUP_TIME * 10 + TARGET_LATENCY * 10, 1, 10);
        assertThat(actualSize).isEqualTo(10);
        actualSize = queryTimeBasedOptimalEstimator.estimate(NODE_STARTUP_TIME * 20 + TARGET_LATENCY * 20, 2, 10);
        assertThat(actualSize).isEqualTo(10);

        // scaleUp 5 times target latency
        actualSize = queryTimeBasedOptimalEstimator.estimate(NODE_STARTUP_TIME * 4 + 5 * 4 * TARGET_LATENCY, 1, 4);
        assertThat(actualSize).isEqualTo(20);
        // scaleUp 2 times target latency
        actualSize = queryTimeBasedOptimalEstimator.estimate(NODE_STARTUP_TIME * 12 + 2 * 12 * TARGET_LATENCY, 3, 4);
        assertThat(actualSize).isEqualTo(8);
    }
}
