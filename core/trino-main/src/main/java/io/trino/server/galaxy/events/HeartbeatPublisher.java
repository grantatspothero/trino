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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
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
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.weakref.jmx.Managed;

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
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class HeartbeatPublisher
        implements AutoCloseable
{
    private static final Logger log = Logger.get(HeartbeatPublisher.class);

    @VisibleForTesting
    public static final JsonCodec<HeartbeatEvent> HEARTBEAT_EVENT_JSON_CODEC = jsonCodec(HeartbeatEvent.class);
    private final AtomicLong lastAcknowledgedPublishNanoTime;
    private final NodeInfo nodeInfo;
    private final String accountId;
    private final String clusterId;
    private final String deploymentId;
    private final String cloudRegionId;
    private final String variant;
    private final String role;
    private final String trinoPlaneFqdn;
    private final String billingTopic;
    private final Duration publishInterval;
    private final Duration terminationGracePeriod;
    private final KafkaPublisher kafkaPublisher;
    private final Map<String, CatalogMetricsSnapshot> lastAcknowledgedPublishCatalogMetrics = new HashMap<>();

    // This must match the event type to use in deserialization in io.starburst.stargate.billing.events.StargateEvent, once
    // we can build Trino with Java 16 and depend on the billing-aggregator (or a broken out model module), this can just be
    // a reference to that enum value
    private static final String HEARTBEAT_EVENT_TYPE_NAME = "HEARTBEAT";

    private final ScheduledExecutorService heartbeatThreadPool = newSingleThreadScheduledExecutor(daemonThreadsNamed("galaxy-billing-heartbeat"));
    private final AtomicReference<ScheduledFuture<?>> heartbeatFuture = new AtomicReference<>();

    @Inject
    public HeartbeatPublisher(NodeInfo nodeInfo, GalaxyConfig galaxyConfig, GalaxyHeartbeatConfig heartbeatConfig, KafkaPublisherConfig kafkaPublisherConfig)
    {
        this(nodeInfo, galaxyConfig, heartbeatConfig, new KafkaPublisherClient(kafkaPublisherConfig));
    }

    @VisibleForTesting
    public HeartbeatPublisher(NodeInfo nodeInfo, GalaxyConfig galaxyConfig, GalaxyHeartbeatConfig heartbeatConfig, KafkaPublisher kafkaPublisher)
    {
        this.lastAcknowledgedPublishNanoTime = new AtomicLong(System.nanoTime());
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(galaxyConfig, "galaxyConfig is null");
        this.accountId = galaxyConfig.getAccountId();
        this.clusterId = galaxyConfig.getClusterId();
        this.deploymentId = galaxyConfig.getDeploymentId();
        this.cloudRegionId = galaxyConfig.getCloudRegionId();
        requireNonNull(heartbeatConfig, "heartbeatConfig is null");
        this.variant = heartbeatConfig.getVariant();
        this.role = heartbeatConfig.getRole();
        this.trinoPlaneFqdn = heartbeatConfig.getTrinoPlaneFqdn();
        this.billingTopic = heartbeatConfig.getBillingTopic();
        this.publishInterval = heartbeatConfig.getPublishInterval();
        this.terminationGracePeriod = heartbeatConfig.getTerminationGracePeriod();
        this.kafkaPublisher = requireNonNull(kafkaPublisher, "kafkaPublisher is null");
    }

    @PostConstruct
    public void start()
    {
        this.heartbeatFuture.set(heartbeatThreadPool.scheduleWithFixedDelay(
                this::tryPublishHeartbeat,
                0,
                publishInterval.toMillis(),
                TimeUnit.MILLISECONDS));
    }

    @PreDestroy
    @Override
    public void close()
    {
        ScheduledFuture<?> future = heartbeatFuture.get();
        if (future != null) {
            future.cancel(false);
        }

        heartbeatThreadPool.execute(this::tryPublishHeartbeat);
        heartbeatThreadPool.shutdown();
        try {
            if (!heartbeatThreadPool.awaitTermination(terminationGracePeriod.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn(prependNodeInfo("Timeout exceeded while publishing billing heartbeat info on shutdown"));
            }
        }
        catch (InterruptedException e) {
            log.error(prependNodeInfo("Failed to publish billing heartbeat info on shutdown"));
            Thread.currentThread().interrupt();
        }

        heartbeatThreadPool.shutdownNow();
        kafkaPublisher.close();
    }

    @VisibleForTesting
    public void tryPublishHeartbeat()
    {
        try {
            publishHeartbeat();
        }
        catch (InterruptedException e) {
            log.info(prependNodeInfo("Shutting down billing heartbeat"));
            Thread.currentThread().interrupt();
        }
        catch (RuntimeException | Error e) {
            log.error(e, prependNodeInfo("Exception caught publishing a heartbeat"));
        }
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
                        monitor.getCrossRegionWriteBytes(),
                        monitor.getPrivateLinkReadBytes(),
                        monitor.getPrivateLinkWriteBytes()))
                .collect(toImmutableList());
    }

    private String prependNodeInfo(String message)
    {
        return "[%s-%s-%s:%s] %s".formatted(accountId, clusterId, deploymentId, nodeInfo.getNodeId(), message);
    }
}
