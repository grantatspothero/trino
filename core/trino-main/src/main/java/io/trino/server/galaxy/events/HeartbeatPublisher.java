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
package io.trino.server.galaxy.events;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import io.trino.galaxy.kafka.KafkaPublisher;
import io.trino.galaxy.kafka.KafkaPublisherClient;
import io.trino.galaxy.kafka.KafkaPublisherConfig;
import io.trino.server.galaxy.GalaxyConfig;
import io.trino.server.galaxy.GalaxyHeartbeatConfig;
import io.trino.spi.galaxy.CatalogNetworkMonitor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.spi.galaxy.CatalogNetworkMonitor.getAllCatalogNetworkMonitors;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class HeartbeatPublisher
        implements AutoCloseable
{
    private static final Logger log = Logger.get(HeartbeatPublisher.class);

    private static final JsonCodec<HeartbeatEvent> HEARTBEAT_EVENT_JSON_CODEC = jsonCodec(HeartbeatEvent.class);
    private final AtomicLong lastAcknowledgedPublishNanoTime;
    private final NodeInfo nodeInfo;
    private final String accountId;
    private final String clusterId;
    private final String deploymentId;
    private final String cloudRegionId;
    private final String variant;
    private final String role;
    private final String trinoPlaneFqdn;
    private final KafkaPublisher kafkaPublisher;
    private final String billingTopic;
    private final Map<String, CatalogMetricsSnapshot> lastAcknowledgedPublishCatalogMetrics = new HashMap<>();

    // This must match the event type to use in deserialization in io.starburst.stargate.billing.events.StargateEvent, once
    // we can build Trino with Java 16 and depend on the billing-aggregator (or a broken out model module), this can just be
    // a reference to that enum value
    private static final String HEARTBEAT_EVENT_TYPE_NAME = "HEARTBEAT";

    private static final int BACKGROUND_THREAD_PERIODIC_DELAY_SECONDS = 30;
    private final ScheduledExecutorService heartbeatThreadPool = newSingleThreadScheduledExecutor(daemonThreadsNamed("galaxy-billing-heartbeat"));
    private final AtomicReference<ScheduledFuture<?>> heartbeatFuture = new AtomicReference<>();

    @Inject
    public HeartbeatPublisher(NodeInfo nodeInfo, GalaxyConfig galaxyConfig, GalaxyHeartbeatConfig heartbeatConfig, KafkaPublisherConfig kafkaPublisherConfig)
    {
        this.lastAcknowledgedPublishNanoTime = new AtomicLong(System.nanoTime());
        this.nodeInfo = nodeInfo;
        this.accountId = galaxyConfig.getAccountId();
        this.clusterId = galaxyConfig.getClusterId();
        this.deploymentId = galaxyConfig.getDeploymentId();
        this.cloudRegionId = galaxyConfig.getCloudRegionId();
        this.variant = heartbeatConfig.getVariant();
        this.role = heartbeatConfig.getRole();
        this.trinoPlaneFqdn = heartbeatConfig.getTrinoPlaneFqdn();
        this.kafkaPublisher = new KafkaPublisherClient(kafkaPublisherConfig);
        this.billingTopic = heartbeatConfig.getBillingTopic();
    }

    @PostConstruct
    public void start()
    {
        this.heartbeatFuture.set(heartbeatThreadPool.scheduleWithFixedDelay(() -> {
            try {
                publishHeartbeat();
            }
            catch (InterruptedException e) {
                log.info("Shutting down billing heartbeat");
                Thread.currentThread().interrupt();
            }
            catch (RuntimeException | Error e) {
                log.error(e, "Exception caught publishing a heartbeat");
            }
        }, 0, BACKGROUND_THREAD_PERIODIC_DELAY_SECONDS, TimeUnit.SECONDS));
    }

    @PreDestroy
    @Override
    public void close()
    {
        ScheduledFuture<?> future = heartbeatFuture.get();
        if (future != null) {
            future.cancel(true);
        }
        heartbeatThreadPool.shutdownNow();
        kafkaPublisher.close();
    }

    private static CloudEvent createCloudEvent(String id, URI source, OffsetDateTime publishTimestamp, HeartbeatEvent heartbeatEvent)
    {
        return new CloudEventBuilder()
                .withId(id)
                .withType(HEARTBEAT_EVENT_TYPE_NAME)
                .withSource(source)
                .withTime(publishTimestamp)
                .withData(HEARTBEAT_EVENT_JSON_CODEC.toJsonBytes(heartbeatEvent))
                .build();
    }

    @Managed
    public boolean isPublishing()
    {
        return heartbeatFuture.get() != null && !heartbeatFuture.get().isCancelled() && !heartbeatFuture.get().isDone();
    }

    @Managed
    public long nanosSinceLastPublish()
    {
        return System.nanoTime() - lastAcknowledgedPublishNanoTime.get();
    }

    private void publishHeartbeat()
            throws InterruptedException
    {
        OffsetDateTime proposedPublishTimestamp = OffsetDateTime.now();

        long eventTimeNanos = System.nanoTime();
        long lastEventTimeNanos = lastAcknowledgedPublishNanoTime.get();
        long durationNanos = eventTimeNanos - lastEventTimeNanos;

        // This is a uri that indicates the source of the event, it's not a specific callable service, but encodes both the pod name and trinoplane so we can
        // easily trace the event back.
        URI source = URI.create(String.format("https://%s.%s/", nodeInfo.getNodeId(), trinoPlaneFqdn));

        List<CatalogMetricsSnapshot> currentCatalogMetricSnapshots = getCurrentCatalogNetworkBytes();

        HeartbeatEvent heartbeatEvent = new HeartbeatEvent(
                NANOSECONDS.toMillis(durationNanos),
                clusterId,
                accountId,
                deploymentId,
                Optional.ofNullable(cloudRegionId).or(() -> Optional.of("n/a")),
                variant,
                role,
                nodeInfo.getNodeId(),
                nodeInfo.getExternalAddress(),
                getCatalogNetworkBytesSinceLastPublish(currentCatalogMetricSnapshots));

        String id = UUID.randomUUID().toString();

        CloudEvent heartbeatCloudEvent = createCloudEvent(id, source, proposedPublishTimestamp, heartbeatEvent);

        byte[] serializedEvent = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE).serialize(heartbeatCloudEvent);

        ListenableFuture<Void> publishFuture = kafkaPublisher.publishPossiblyBlocking(new ProducerRecord<>(billingTopic, serializedEvent));
        kafkaPublisher.flush();
        getDone(publishFuture);

        lastAcknowledgedPublishNanoTime.compareAndSet(lastEventTimeNanos, eventTimeNanos);
        currentCatalogMetricSnapshots.forEach(catalogMetricsSnapshot -> lastAcknowledgedPublishCatalogMetrics.replace(catalogMetricsSnapshot.getCatalogName(), catalogMetricsSnapshot));
    }

    private List<CatalogMetricsSnapshot> getCatalogNetworkBytesSinceLastPublish(List<CatalogMetricsSnapshot> allCatalogMetrics)
    {
        if (allCatalogMetrics.isEmpty()) {
            return ImmutableList.of();
        }
        // get diff from last published network bytes
        List<CatalogMetricsSnapshot> bytesSinceLastPublish = allCatalogMetrics
                .stream()
                .map(catalogMetricsSnapshot -> catalogMetricsSnapshot.minus(lastAcknowledgedPublishCatalogMetrics.computeIfAbsent(catalogMetricsSnapshot.getCatalogId(), catalogId -> new CatalogMetricsSnapshot(catalogId, catalogMetricsSnapshot.getCatalogName()))))
                .collect(toImmutableList());

        return bytesSinceLastPublish;
    }

    private static List<CatalogMetricsSnapshot> getCurrentCatalogNetworkBytes()
    {
        Collection<CatalogNetworkMonitor> allMonitors = getAllCatalogNetworkMonitors();

        return allMonitors
                .stream()
                .map(monitor -> new CatalogMetricsSnapshot(
                        monitor.getCatalogId(),
                        monitor.getCatalogName(),
                        monitor.getIntraRegionReadBytes(),
                        monitor.getIntraRegionWriteBytes(),
                        monitor.getCrossRegionReadBytes(),
                        monitor.getCrossRegionWriteBytes()))
                .collect(toImmutableList());
    }
}
