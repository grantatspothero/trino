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
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.URI;
import java.time.OffsetDateTime;

import static java.util.Objects.requireNonNull;

public class GalaxyKafkaEventListener
        implements EventListener
{
    private static final String QUERY_COMPLETED_EVENT_TYPE = "trino.query.event.completed";
    private static final EventFormat CLOUD_EVENT_JSON_FORMAT = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);

    private final String accountId;
    private final String clusterId;
    private final String deploymentId;
    private final URI trinoPlaneFqdn;
    private final String eventKafkaTopic;
    private final JsonCodec<GalaxyQueryCompletedEvent> eventJsonCodec;
    private final AsyncKafkaPublisher kafkaPublisher;

    @Inject
    public GalaxyKafkaEventListener(GalaxyKafkaEventListenerConfig config, KafkaPublisherConfig publisherConfig, JsonCodec<GalaxyQueryCompletedEvent> eventJsonCodec)
    {
        requireNonNull(config, "config is null");
        accountId = config.getAccountId();
        clusterId = config.getClusterId();
        deploymentId = config.getDeploymentId();
        trinoPlaneFqdn = URI.create(config.getTrinoPlaneFqdn());
        eventKafkaTopic = config.getEventKafkaTopic();
        this.eventJsonCodec = requireNonNull(eventJsonCodec, "eventJsonCodec is null");
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
                .withData(eventJsonCodec.toJsonBytes(galaxyEvent))
                .build();
        byte[] bytes = CLOUD_EVENT_JSON_FORMAT.serialize(cloudEvent);

        kafkaPublisher.submit(new KafkaRecord(eventKafkaTopic, bytes));
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
