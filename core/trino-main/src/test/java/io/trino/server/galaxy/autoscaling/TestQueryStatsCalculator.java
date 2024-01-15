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

import com.google.common.collect.ImmutableSet;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.QueryManagerStats;
import io.trino.execution.QueryState;
import io.trino.operator.RetryPolicy;
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.server.galaxy.autoscaling.WorkerRecommendationProvider.QueryStats;
import io.trino.spi.QueryId;
import io.trino.spi.resourcegroups.ResourceGroupId;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static io.airlift.units.Duration.succinctDuration;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.BlockedReason.WAITING_FOR_MEMORY;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class TestQueryStatsCalculator
{
    @Test
    void calculateRunningQueryStats()
    {
        TestingTicker ticker = new TestingTicker();

        List<BasicQueryInfo> queryInfos = new ArrayList<>();
        queryInfos.add(createQueryInfo("test_1", QueryState.RUNNING, new Duration(30, MINUTES)));
        queryInfos.add(createQueryInfo("test_2", QueryState.RUNNING, new Duration(30, MINUTES)));
        queryInfos.add(createQueryInfo("test_4", QueryState.PLANNING, new Duration(10, MINUTES)));

        queryInfos.add(createQueryInfo("test_3", QueryState.FINISHED, new Duration(5, MINUTES)));
        queryInfos.add(createQueryInfo("test_5", QueryState.QUEUED, new Duration(0, MINUTES)));
        queryInfos.add(createQueryInfo("test_6", QueryState.QUEUED, new Duration(0, MINUTES)));
        queryInfos.add(createQueryInfo("test_7", QueryState.WAITING_FOR_RESOURCES, new Duration(0, MINUTES)));

        QueryStatsCalculator calculator = new QueryStatsCalculator(
                ticker,
                QueryManagerStats::new,
                () -> queryInfos,
                () -> 1);

        calculator.calculateQueryStats();

        calculator.getAge();
        QueryStats runningQueryStats = calculator.getRunningQueryStats();
        assertThat(runningQueryStats.getAverageCpuTimePerQueryMillis()).isEqualTo(MINUTES.toMillis(30 + 30 + 10) / 3.0);

        long queuedQueries = calculator.getQueuedQueries();
        assertThat(queuedQueries).isEqualTo(3);
    }

    @Test
    void calculateParallelism()
    {
        TestingTicker ticker = new TestingTicker();

        List<BasicQueryInfo> queryInfos = new ArrayList<>();
        queryInfos.add(createQueryInfo("test_1", QueryState.RUNNING, new Duration(1, MINUTES)));
        queryInfos.add(createQueryInfo("test_2", QueryState.RUNNING, new Duration(2, MINUTES)));

        QueryStatsCalculator calculator = new QueryStatsCalculator(
                ticker,
                QueryManagerStats::new,
                () -> queryInfos,
                () -> 1);

        ticker.increment(1, MINUTES);
        Duration age = calculator.getAge();
        assertThat(age).isEqualTo(succinctDuration(1, MINUTES));

        calculator.calculateQueryStats();

        age = calculator.getAge();
        assertThat(age).isEqualTo(succinctDuration(0, MINUTES));
        double avgWorkerParallelism = calculator.getAverageWorkerParallelism();
        assertThat(avgWorkerParallelism).isEqualTo(1 + 2);
        assertThat(calculator.getRunningQueryStats().getAverageCpuTimePerQueryMillis()).isEqualTo(MINUTES.toMillis(1 + 2) / 2.0);

        // another minute, another round
        ticker.increment(1, MINUTES);
        queryInfos.clear();
        queryInfos.add(createQueryInfo("test_1", QueryState.RUNNING, new Duration(2, MINUTES)));
        queryInfos.add(createQueryInfo("test_2", QueryState.RUNNING, new Duration(4, MINUTES)));

        calculator.calculateQueryStats();

        avgWorkerParallelism = calculator.getAverageWorkerParallelism();
        assertThat(avgWorkerParallelism).isEqualTo(1 + 2);
        assertThat(calculator.getRunningQueryStats().getAverageCpuTimePerQueryMillis()).isEqualTo(MINUTES.toMillis(2 + 4) / 2.0);

        // another minute, another round
        ticker.increment(1, MINUTES);
        queryInfos.clear();
        queryInfos.add(createQueryInfo("test_1", QueryState.RUNNING, new Duration(12, MINUTES)));
        queryInfos.add(createQueryInfo("test_2", QueryState.RUNNING, new Duration(14, MINUTES)));

        calculator.calculateQueryStats();
        avgWorkerParallelism = calculator.getAverageWorkerParallelism();
        // parallelism was 3, but in current minute it is 10, so the weighed value should be a little bit lower than 10
        assertThat(avgWorkerParallelism).isEqualTo(10, within(0.3));
    }

    private static BasicQueryInfo createQueryInfo(String queryId, QueryState state, Duration totalCpuTime)
    {
        return new BasicQueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                Optional.of(new ResourceGroupId("global")),
                state,
                true,
                URI.create("1"),
                "",
                Optional.empty(),
                Optional.empty(),
                new BasicQueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        new Duration(8, MINUTES),
                        new Duration(7, MINUTES),
                        new Duration(34, MINUTES),
                        99,
                        13,
                        14,
                        15,
                        100,
                        DataSize.valueOf("21GB"),
                        22,
                        DataSize.valueOf("23GB"),
                        24,
                        25,
                        DataSize.valueOf("26GB"),
                        DataSize.valueOf("27GB"),
                        DataSize.valueOf("28GB"),
                        DataSize.valueOf("29GB"),
                        totalCpuTime,
                        new Duration(31, MINUTES),
                        new Duration(32, MINUTES),
                        new Duration(33, MINUTES),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        OptionalDouble.of(20),
                        OptionalDouble.of(0)),
                null,
                null,
                Optional.empty(),
                RetryPolicy.NONE);
    }
}
