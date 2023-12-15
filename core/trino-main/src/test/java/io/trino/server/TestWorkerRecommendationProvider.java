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

import io.airlift.units.Duration;
import org.assertj.core.api.DoubleAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.trino.server.WorkerRecommendationProvider.QueryStats;
import static io.trino.server.WorkerRecommendationProvider.estimateCpuTimeToProcessQueriesMillis;
import static org.assertj.core.api.Assertions.assertThat;

class TestWorkerRecommendationProvider
{
    private static Stream<Arguments> provideForNoQueue()
    {
        return Stream.of(
                Arguments.of("Single Query, no Queue, no History, time should equal current_running_time",
                        new QueryStats(0, 0),
                        new QueryStats(1, 100),
                        isEqualTo(100)),
                Arguments.of("Single Query, no Queue, time should equal current_running_time",
                        new QueryStats(1, 100),
                        new QueryStats(1, 100),
                        isEqualTo(100)),
                Arguments.of("Many Queries, no Queue, no History, time should equal current_running_time",
                        new QueryStats(0, 0),
                        new QueryStats(40, 100),
                        isEqualTo(100)),
                Arguments.of("Many Queries, no Queue, current avg runtime longer than past, time should equal current_running_time",
                        new QueryStats(1, 10),
                        new QueryStats(10, 100),
                        isEqualTo(100)),
                Arguments.of("Many Queries, no Queue, current avg runtime shorter than past, time should be between past_avg_time and current_avg  ",
                        new QueryStats(1, 30),
                        new QueryStats(10, 100),
                        isBetween(10 * 10, 30 * 10)));
    }

    @ParameterizedTest
    @MethodSource("provideForNoQueue")
    void noQueue(String name, QueryStats pastQueries, QueryStats currentQueries, Consumer<DoubleAssert> assertFunction)
    {
        double expectedRunningTimeSecs = estimateCpuTimeToProcessQueriesMillis(pastQueries, currentQueries, 0);
        assertFunction.accept((DoubleAssert) assertThat(expectedRunningTimeSecs).describedAs(name));
    }

    private static Stream<Arguments> provideForQueueNoHistory()
    {
        double avgQueryTime = 10;
        double expectedRunningQueryTime = 10 * avgQueryTime;

        return Stream.of(
                Arguments.of("Queue, no History, time should equal current_running_time * 2 + avgQueryTime",
                        new QueryStats(10, 10 * avgQueryTime),
                        1,
                        expectedRunningQueryTime + avgQueryTime),
                Arguments.of("Queue, no History, time should equal current_running_time * 2 + avgQueryTime",
                        new QueryStats(10, 10 * avgQueryTime),
                        42,
                        expectedRunningQueryTime + 42 * avgQueryTime));
    }

    @ParameterizedTest
    @MethodSource("provideForQueueNoHistory")
    void queueNoHistory(String name, QueryStats currentQueries, long queryQueueCount, double expectedRunningTime)
    {
        final QueryStats pastQueryStats = new QueryStats(0, 0);
        double time = estimateCpuTimeToProcessQueriesMillis(pastQueryStats, currentQueries, queryQueueCount);
        assertThat(time).describedAs(name).isEqualTo(expectedRunningTime);
    }

    private static Stream<Arguments> provideForQueueWithHistory()
    {
        double avgCurrentQueryTimeLow = 10;
        double avgCurrentQueryTimeHigh = 300;
        double avgHistoricalQueryTime = 100;

        int currentRunningQueryCount = 10;
        int queuedQueriesCount = 13;
        return Stream.of(
                Arguments.of("historical query time higher than current, expected time should equal currentQueryCnt * avgHistoricalQueryTime",
                        new QueryStats(100, 100 * avgHistoricalQueryTime),
                        new QueryStats(currentRunningQueryCount, currentRunningQueryCount * avgCurrentQueryTimeLow),
                        0,
                        isBetween(currentRunningQueryCount * avgCurrentQueryTimeLow,
                                currentRunningQueryCount * avgHistoricalQueryTime)),
                Arguments.of("historical query time higher than current, expected time should equal currentQueryCnt * avgHistoricalQueryTime + estimatedQueryTime",
                        new QueryStats(100, 100 * avgHistoricalQueryTime),
                        new QueryStats(currentRunningQueryCount, currentRunningQueryCount * avgCurrentQueryTimeLow),
                        queuedQueriesCount,
                        isBetween(currentRunningQueryCount * avgCurrentQueryTimeLow + queuedQueriesCount * avgHistoricalQueryTime,
                                currentRunningQueryCount * avgHistoricalQueryTime + queuedQueriesCount * avgHistoricalQueryTime)),
                Arguments.of("historical query time lower than current, expected time should equal currentQueryCnt * avgCurrentQueryTimeHigh",
                        new QueryStats(100, 100 * avgHistoricalQueryTime),
                        new QueryStats(currentRunningQueryCount, currentRunningQueryCount * avgCurrentQueryTimeHigh),
                        0,
                        isEqualTo(currentRunningQueryCount * avgCurrentQueryTimeHigh)),
                Arguments.of("historical query time lower than current, expected time should equal currentQueryCnt * avgCurrentQueryTimeHigh+ avgQueryTime",
                        new QueryStats(100, 100 * avgHistoricalQueryTime),
                        new QueryStats(currentRunningQueryCount, currentRunningQueryCount * avgCurrentQueryTimeHigh),
                        queuedQueriesCount,
                        isEqualTo(currentRunningQueryCount * avgCurrentQueryTimeHigh + queuedQueriesCount * avgHistoricalQueryTime)));
    }

    @ParameterizedTest
    @MethodSource("provideForQueueWithHistory")
    void queueWithHistory(String name, QueryStats pastQueryStats, QueryStats runningQueryStats, int queryQueueSize, Consumer<DoubleAssert> assertFunction)
    {
        double time = estimateCpuTimeToProcessQueriesMillis(pastQueryStats, runningQueryStats, queryQueueSize);
        assertFunction.accept((DoubleAssert) assertThat(time).describedAs(name));
    }

    private static Stream<Arguments> provideForVerifyScaleup()
    {
        // 120 seconds on 10 nodes cluster
        long cpuTimeToProcessQueriesMillis = 120 * 1000 * 10;

        return Stream.of(
                Arguments.of("scaleUpTime lower than running time, can resize", cpuTimeToProcessQueriesMillis, 0, 0, 15),
                Arguments.of("scaleUpTime lower than running time, can resize", cpuTimeToProcessQueriesMillis, 60, 0, 15),
                Arguments.of("scaleUpTime the same as running time, should not resize", cpuTimeToProcessQueriesMillis, 120, 0, 10),
                Arguments.of("scaleUpTime higher than running time, should not resize", cpuTimeToProcessQueriesMillis, 200, 0, 10),
                // verify minimalClusterTime behavior
                Arguments.of("scaleUpTime low, minimalClusterRuntime lower than running time, can resize", cpuTimeToProcessQueriesMillis, 60, 30, 15),
                Arguments.of("scaleUpTime low, minimalClusterRuntime higher than running time, should not resize", cpuTimeToProcessQueriesMillis, 60, 60, 10),
                Arguments.of("scaleUpTime low, minimalClusterRuntime higher than running time, should not resize", cpuTimeToProcessQueriesMillis, 60, 120, 10),
                Arguments.of("expectedRunningTime higher than  scaleUpTime + minimalClusterRuntime, can resize", 1200 * 1000 * 10, 60, 30, 15));
    }

    @ParameterizedTest
    @MethodSource("provideForVerifyScaleup")
    void verifyScaleup(String name, long cpuTimeToProcessQueriesMillis, long scaleUpTimeSecs, long minimalClusterRuntimeSecs, long expectedSize)
    {
        GalaxyTrinoAutoscalingConfig cfg = new GalaxyTrinoAutoscalingConfig();
        cfg.setNodeStartupTime(Duration.succinctDuration(scaleUpTimeSecs, TimeUnit.SECONDS));
        cfg.setRemainingTimeScaleUpThreshold(Duration.succinctDuration(minimalClusterRuntimeSecs, TimeUnit.SECONDS));
        cfg.setRemainingTimeScaleDownThreshold(Duration.ZERO);

        QueryTimeRatioBasedEstimator querytimeRatioBasedEstimator = new QueryTimeRatioBasedEstimator(cfg);
        int actualSize = querytimeRatioBasedEstimator.estimate(cpuTimeToProcessQueriesMillis, 1, 10);
        assertThat(actualSize).describedAs(name).isEqualTo(expectedSize);
    }

    @Test
    void shouldRescaleA()
    {
        int actualSize = 0;
        GalaxyTrinoAutoscalingConfig cfg = new GalaxyTrinoAutoscalingConfig();
        cfg.setNodeStartupTime(Duration.succinctDuration(60, TimeUnit.SECONDS));
        cfg.setRemainingTimeScaleUpThreshold(Duration.succinctDuration(60, TimeUnit.SECONDS));
        cfg.setRemainingTimeScaleDownThreshold(Duration.succinctDuration(30, TimeUnit.SECONDS));

        QueryTimeRatioBasedEstimator querytimeRatioBasedEstimator = new QueryTimeRatioBasedEstimator(cfg);

        // -- scaleDown
        actualSize = querytimeRatioBasedEstimator.estimate(30_000 * 10, 1, 10);
        assertThat(actualSize).isEqualTo(10);

        actualSize = querytimeRatioBasedEstimator.estimate(10_000 * 10, 1, 10);
        assertThat(actualSize).isEqualTo(8);
    }

    static Consumer<DoubleAssert> isBetween(double a, double b)
    {
        return (d) -> d.isBetween(a, b);
    }

    static Consumer<DoubleAssert> isEqualTo(double expected)
    {
        return (d) -> d.isEqualTo(expected);
    }
}
