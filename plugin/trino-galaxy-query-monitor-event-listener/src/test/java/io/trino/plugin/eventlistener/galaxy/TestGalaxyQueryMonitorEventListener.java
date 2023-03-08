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
package io.trino.plugin.eventlistener.galaxy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.session.ResourceEstimates;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static org.testng.Assert.assertEquals;

public class TestGalaxyQueryMonitorEventListener
{
    @Test
    public void testQueryCompleted()
    {
        List<LogEntry> actual = new ArrayList<>();

        Logger logger = Logger.getLogger("io.trino.plugin.eventlistener.galaxy.GalaxyQueryMonitorEventListener");
        Handler handler = new Handler()
        {
            @Override
            public void publish(LogRecord record)
            {
                actual.add(new LogEntry(record.getLoggerName(), record.getLevel(), record.getMessage()));
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        logger.addHandler(handler);

        List<LogEntry> expected = new ArrayList<>();
        GalaxyQueryMonitorEventListener listener = new GalaxyQueryMonitorEventListener();
        for (String retryPolicy : Set.of("NONE", "TASK", "QUERY")) {
            for (ErrorType errorType : ErrorType.values()) {
                QueryCompletedEvent event = createEvent(retryPolicy, Optional.of(errorType));
                listener.queryCompleted(event);
                expected.add(new LogEntry(
                        "io.trino.plugin.eventlistener.galaxy.GalaxyQueryMonitorEventListener.RETRY_POLICY_%s.FAILED_WITH_%s".formatted(retryPolicy, errorType),
                        INFO,
                        "Query %s failed: %s ({\"sample\": \"failure_json\"})".formatted(event.getMetadata().getQueryId(), event.getFailureInfo().orElseThrow().getErrorCode().getName())));
            }
            QueryCompletedEvent event = createEvent(retryPolicy, Optional.empty());
            listener.queryCompleted(event);
            expected.add(new LogEntry(
                    "io.trino.plugin.eventlistener.galaxy.GalaxyQueryMonitorEventListener.RETRY_POLICY_%s.FINISHED".formatted(retryPolicy),
                    INFO,
                    "Query %s finished successfully".formatted(event.getMetadata().getQueryId())));
        }
        listener.queryCompleted(createEvent("INVALID", Optional.empty()));
        expected.add(new LogEntry(
                "io.trino.plugin.eventlistener.galaxy.GalaxyQueryMonitorEventListener",
                SEVERE,
                "Unexpected retry policy: INVALID"));

        logger.removeHandler(handler);

        assertEquals(actual, expected);
    }

    private record LogEntry(String loggerName, Level logLevel, String message)
    {
        public LogEntry
        {
            requireNonNull(loggerName, "loggerName is null");
            requireNonNull(logLevel, "logLevel is null");
            requireNonNull(message, "message is null");
        }
    }

    private static QueryCompletedEvent createEvent(String retryPolicy, Optional<ErrorType> errorType)
    {
        return new QueryCompletedEvent(
                new QueryMetadata(
                        "query_%s_%s".formatted(retryPolicy, errorType),
                        Optional.empty(),
                        "SELECT 1",
                        Optional.empty(),
                        Optional.empty(),
                        "FINISHED",
                        ImmutableList.of(),
                        ImmutableList.of(),
                        URI.create("fake://uri"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                new QueryStatistics(
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        ImmutableList.of(),
                        1,
                        true,
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        Optional.empty()),
                new QueryContext(
                        "user",
                        Optional.empty(),
                        ImmutableSet.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableSet.of(),
                        ImmutableSet.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(),
                        new ResourceEstimates(
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        "127.0.0.1",
                        "version",
                        "test",
                        Optional.empty(),
                        retryPolicy),
                new QueryIOMetadata(
                        ImmutableList.of(),
                        Optional.empty()),
                errorType.map(type -> new QueryFailureInfo(
                        new ErrorCode(
                                type.ordinal(),
                                "TESTING_" + type,
                                type),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        "{\"sample\": \"failure_json\"}")),
                ImmutableList.of(),
                Instant.now(),
                Instant.now(),
                Instant.now());
    }
}
