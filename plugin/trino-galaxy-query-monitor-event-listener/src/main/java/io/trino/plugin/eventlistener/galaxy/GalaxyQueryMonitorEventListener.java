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

import io.airlift.log.Logger;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Event listener designed to emit log entries upon query completion into a dedicated logging category for a specific retry policy and an error type.
 * These log events can be used for building metrics of successful and failed queries categorized based on error type.
 * A monitoring system (such as Datadog) will be monitoring the number of records in each of the logger category and send an alert when a certain
 * number of log records is emitted over a certain period.
 */
public class GalaxyQueryMonitorEventListener
        implements EventListener
{
    private static final Logger log = Logger.get(GalaxyQueryMonitorEventListener.class);

    private final QueryLogger logger = new QueryLogger();

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        Optional<RetryPolicy> retryPolicy = RetryPolicy.optionalValueOf(event.getContext().getRetryPolicy());
        if (retryPolicy.isEmpty()) {
            log.error("Unexpected retry policy: %s", event.getContext().getRetryPolicy());
            return;
        }
        logger.log(event.getMetadata().getQueryId(), retryPolicy.get(), event.getFailureInfo());
    }

    private enum RetryPolicy
    {
        TASK,
        QUERY,
        NONE,
        /**/;

        public static Optional<RetryPolicy> optionalValueOf(String value)
        {
            for (RetryPolicy policy : values()) {
                if (policy.toString().equals(value)) {
                    return Optional.of(policy);
                }
            }
            return Optional.empty();
        }
    }

    private static class QueryLogger
    {
        private static final Map<String, Logger> loggers = new ConcurrentHashMap<>();

        private static String getLoggerName(RetryPolicy retryPolicy, Optional<ErrorType> errorType)
        {
            return errorType.map(type -> "%s.RETRY_POLICY_%s.FAILED_WITH_%s".formatted(GalaxyQueryMonitorEventListener.class.getName(), retryPolicy, type))
                    .orElseGet(() -> "%s.RETRY_POLICY_%s.FINISHED".formatted(GalaxyQueryMonitorEventListener.class.getName(), retryPolicy));
        }

        public void log(String queryId, RetryPolicy retryPolicy, Optional<QueryFailureInfo> failureInfo)
        {
            String loggerName = getLoggerName(retryPolicy, failureInfo.map(QueryFailureInfo::getErrorCode).map(ErrorCode::getType));
            Logger logger = loggers.computeIfAbsent(loggerName, Logger::get);
            if (failureInfo.isPresent()) {
                logger.info("Query %s failed: %s (%s)", queryId, failureInfo.get().getErrorCode().getName(), failureInfo.get().getFailuresJson());
            }
            else {
                logger.info("Query %s finished successfully", queryId);
            }
        }
    }
}
