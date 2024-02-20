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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import io.trino.galaxy.kafka.AsyncKafkaPublisher;
import io.trino.galaxy.kafka.KafkaPublisherConfig;
import io.trino.galaxy.kafka.KafkaRecord;
import io.trino.plugin.eventlistener.galaxy.event.GalaxyQueryCompletedEvent;
import io.trino.plugin.eventlistener.galaxy.event.GalaxyQueryLifeCycleEvent;
import io.trino.spi.ErrorCode;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.QueryMetadata;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GalaxyKafkaEventListener
        implements EventListener
{
    private static final String QUERY_COMPLETED_EVENT_TYPE = "trino.query.event.completed";
    private static final String QUERY_LIFECYCLE_EVENT_TYPE = "trino.query.event.lifecycle";
    private static final EventFormat CLOUD_EVENT_JSON_FORMAT = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);

    private final String accountId;
    private final String clusterId;
    private final String deploymentId;
    private final URI trinoPlaneFqdn;
    private final String eventKafkaTopic;
    private final Optional<String> lifeCycleEventKafkaTopic;
    private final JsonCodec<GalaxyQueryCompletedEvent> completedEventJsonCodec;
    private final JsonCodec<GalaxyQueryLifeCycleEvent> lifeCycleEventJsonCodec;
    private final AsyncKafkaPublisher kafkaPublisher;

    @Inject
    public GalaxyKafkaEventListener(
            GalaxyKafkaEventListenerConfig config,
            KafkaPublisherConfig publisherConfig,
            JsonCodec<GalaxyQueryCompletedEvent> completedEventJsonCodec,
            JsonCodec<GalaxyQueryLifeCycleEvent> lifeCycleEventJsonCodec)
    {
        requireNonNull(config, "config is null");
        accountId = config.getAccountId();
        clusterId = config.getClusterId();
        deploymentId = config.getDeploymentId();
        trinoPlaneFqdn = URI.create(config.getTrinoPlaneFqdn());
        eventKafkaTopic = config.getEventKafkaTopic();
        lifeCycleEventKafkaTopic = config.getLifeCycleEventKafkaTopic();
        this.completedEventJsonCodec = requireNonNull(completedEventJsonCodec, "completedEventJsonCodec is null");
        this.lifeCycleEventJsonCodec = requireNonNull(lifeCycleEventJsonCodec, "lifeCycleEventJsonCodec is null");
        kafkaPublisher = new AsyncKafkaPublisher(config.getPluginReportingName(), config.getMaxBufferingCapacity(), publisherConfig);
    }

    @PostConstruct
    public void initialize()
    {
        kafkaPublisher.initialize();
    }

    @PreDestroy
    public void destroy()
    {
        kafkaPublisher.destroy();
    }

    @Override
    public void queryCreated(QueryCreatedEvent event)
    {
        // QueryState = QUEUED
        publishLifeCycleEvent(event.getCreateTime(), event.getMetadata(), event.getContext(), Optional.empty());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        GalaxyQueryCompletedEvent galaxyEvent = new GalaxyQueryCompletedEvent(accountId, clusterId, deploymentId, event);

        // Use the event type and query ID as the unique message identifier
        String id = QUERY_COMPLETED_EVENT_TYPE + "." + event.getMetadata().getQueryId();

        CloudEvent cloudEvent = new CloudEventBuilder()
                .withId(id)
                .withType(QUERY_COMPLETED_EVENT_TYPE)
                .withSource(trinoPlaneFqdn)
                .withTime(OffsetDateTime.now())
                .withData(completedEventJsonCodec.toJsonBytes(galaxyEvent))
                .build();
        byte[] bytes = CLOUD_EVENT_JSON_FORMAT.serialize(cloudEvent);

        kafkaPublisher.submit(new KafkaRecord(eventKafkaTopic, bytes));

        // QueryState = FINISHED or FAILED
        publishLifeCycleEvent(event.getEndTime(), event.getMetadata(), event.getContext(), event.getFailureInfo().map(QueryFailureInfo::getErrorCode));
    }

    private void publishLifeCycleEvent(Instant eventTime, QueryMetadata metadata, QueryContext context, Optional<ErrorCode> errorCode)
    {
        lifeCycleEventKafkaTopic.ifPresent(topic -> {
            // Use the event type, query ID and query state as the unique message identifier
            String id = "%s.%s.%s".formatted(QUERY_LIFECYCLE_EVENT_TYPE, metadata.getQueryId(), metadata.getQueryState());

            GalaxyQueryLifeCycleEvent lifeCycleEvent = new GalaxyQueryLifeCycleEvent(
                    accountId,
                    clusterId,
                    deploymentId,
                    metadata.getQueryId(),
                    metadata.getQueryState(),
                    eventTime,
                    metadata.getQuery(),
                    context.getPrincipal(),
                    errorCode);
            CloudEvent lifeCycleCloudEvent = new CloudEventBuilder()
                    .withId(id)
                    .withType(QUERY_LIFECYCLE_EVENT_TYPE)
                    .withSource(trinoPlaneFqdn)
                    .withTime(OffsetDateTime.now())
                    .withData(lifeCycleEventJsonCodec.toJsonBytes(lifeCycleEvent))
                    .build();
            kafkaPublisher.submit(new KafkaRecord(topic, CLOUD_EVENT_JSON_FORMAT.serialize(lifeCycleCloudEvent)));
        });
    }

    @Managed
    public int getQueueSize()
    {
        return kafkaPublisher.getQueueSize();
    }

    @Managed
    public int getCapacitySaturated()
    {
        return kafkaPublisher.isCapacitySaturated() ? 1 : 0;
    }

    @Managed
    @Nested
    public DistributionStat getPayloadByteSize()
    {
        return kafkaPublisher.getPayloadByteSize();
    }

    @Managed
    @Nested
    public CounterStat getFailedRecordErrorRetries()
    {
        return kafkaPublisher.getFailedRecordErrorRetries();
    }

    @Managed
    @Nested
    public CounterStat getPayloadTooLargeDroppedRecords()
    {
        return kafkaPublisher.getPayloadTooLargeDroppedRecords();
    }

    @Managed
    @Nested
    public CounterStat getQueueOverflowDroppedRecords()
    {
        return kafkaPublisher.getQueueOverflowDroppedRecords();
    }
}
