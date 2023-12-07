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
package io.trino.server.galaxy;

import com.google.common.io.Closer;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import io.trino.galaxy.kafka.KafkaRecord;
import io.trino.galaxy.kafka.TestingKafkaPublisher;
import io.trino.server.galaxy.events.CatalogMetricsSnapshot;
import io.trino.server.galaxy.events.HeartbeatEvent;
import io.trino.server.galaxy.events.HeartbeatPublisher;
import io.trino.spi.galaxy.CatalogNetworkMonitor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.trino.server.galaxy.events.HeartbeatPublisher.HEARTBEAT_EVENT_JSON_CODEC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestGalaxyHeartbeatPublisher
{
    private static final int CHUNK_SIZE = 64;
    private static final int MAX_CROSS_REGION_BYTES = CHUNK_SIZE * 2;

    @Test
    public void testHeartbeatPublisherOnShutdown()
            throws Exception
    {
        try (Closer closer = Closer.create()) {
            TestingKafkaPublisher kafkaPublisher = new TestingKafkaPublisher();
            closer.register(kafkaPublisher::close);
            HeartbeatPublisher heartbeatPublisher = createTestingHeartbeatPublisher(kafkaPublisher);
            heartbeatPublisher.start();

            // wait for initial publish, which shouldn't contain any catalog metrics
            kafkaPublisher.awaitPublish(records -> records.size() > 0, 1000);

            CatalogNetworkMonitor catalogNetworkMonitor = CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor("foocatalog", "c-1234567890", MAX_CROSS_REGION_BYTES, MAX_CROSS_REGION_BYTES);
            try (InputStream baseInputStream = new ByteArrayInputStream(new byte[CHUNK_SIZE])) {
                // create monitored input stream, read a certain amount of bytes, and verify that the amount read is correctly reported
                InputStream monitoredInputStream = catalogNetworkMonitor.monitorInputStream(true, baseInputStream);

                monitoredInputStream.readNBytes(CHUNK_SIZE);
                long readBytes = catalogNetworkMonitor.getCrossRegionReadBytes();
                assertThat(readBytes).isEqualTo(CHUNK_SIZE);
            }

            // publish updated catalog metrics
            heartbeatPublisher.tryPublishHeartbeat();
            kafkaPublisher.awaitPublish(records -> records.size() > 1, 1000);
            List<KafkaRecord> publishedRecords = kafkaPublisher.getPublishedRecords();
            assertThat(publishedRecords).hasSize(2);

            // deserialize KafkaRecord
            for (KafkaRecord record : publishedRecords) {
                HeartbeatEvent heartbeatEvent = deserializeHeartbeatEvent(record);
                List<CatalogMetricsSnapshot> catalogMetrics = heartbeatEvent.getCatalogMetrics();
                if (catalogMetrics.size() <= 0) {
                    continue;
                }

                // verify round trip of accurate catalog metrics
                assertThat(catalogMetrics.size()).isEqualTo(1);
                long crossRegionReadBytes = sumCrossRegionReadBytesForCatalogMetrics(catalogMetrics);
                assertThat(crossRegionReadBytes).isEqualTo(CHUNK_SIZE);
            }

            // verify final publish on close
            assertThat(kafkaPublisher.getPublishedRecords()).hasSize(2);
            heartbeatPublisher.close();
            kafkaPublisher.awaitPublish(records -> records.size() > 2, 1000);
        }
    }

    private static HeartbeatPublisher createTestingHeartbeatPublisher(TestingKafkaPublisher kafkaPublisher)
    {
        GalaxyConfig galaxyConfig = new GalaxyConfig()
                .setAccountId("a-12345678")
                .setClusterId("w-1234567890")
                .setDeploymentId("dep-123456789012")
                .setCloudRegionId("aws-us-east1");
        GalaxyHeartbeatConfig heartbeatConfig = new GalaxyHeartbeatConfig()
                .setVariant("standard")
                .setRole("worker")
                .setTrinoPlaneFqdn("acme-sample.trino.local.gate0.net")
                .setPublishInterval(new Duration(9999, TimeUnit.SECONDS));
        return new HeartbeatPublisher(new NodeInfo("foo"), galaxyConfig, heartbeatConfig, kafkaPublisher);
    }

    private static HeartbeatEvent deserializeHeartbeatEvent(KafkaRecord kafkaRecord)
    {
        CloudEvent cloudEvent = requireNonNull(EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE)).deserialize(kafkaRecord.getPayload());
        return HEARTBEAT_EVENT_JSON_CODEC.fromJson(requireNonNull(cloudEvent.getData()).toBytes());
    }

    private static long sumCrossRegionReadBytesForCatalogMetrics(List<CatalogMetricsSnapshot> catalogMetrics)
    {
        return catalogMetrics.stream()
                .map(CatalogMetricsSnapshot::getCrossRegionReadBytes)
                .reduce(0L, Long::sum);
    }
}
