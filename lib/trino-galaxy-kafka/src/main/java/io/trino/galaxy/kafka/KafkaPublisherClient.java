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
package io.trino.galaxy.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class KafkaPublisherClient
        implements KafkaPublisher
{
    private static final Logger log = Logger.get(KafkaPublisherClient.class);

    @VisibleForTesting
    protected static final Predicate<Throwable> UNABLE_TO_RESOLVE_PREDICATE = throwable ->
            Throwables.getCausalChain(throwable).stream()
                    .anyMatch(t -> t instanceof ConfigException && t.getMessage().contains("No resolvable bootstrap urls given"));

    private final Producer<byte[], byte[]> producer;

    public KafkaPublisherClient(KafkaPublisherConfig config)
    {
        AtomicLong resolveRetries = new AtomicLong(0);
        this.producer = Failsafe.with(
                        RetryPolicy.builder()
                                .handleIf(UNABLE_TO_RESOLVE_PREDICATE::test)
                                .withMaxRetries(Integer.MAX_VALUE)
                                .withMaxDuration(Duration.of((long) config.getKafkaBootstrapServersResolutionTimeout().getValue(), config.getKafkaBootstrapServersResolutionTimeout().getUnit().toChronoUnit()))
                                .withDelay(Duration.ofSeconds(1), Duration.ofSeconds(5))
                                .onRetry(ignore -> resolveRetries.incrementAndGet())
                                .build())
                .get(() -> new KafkaProducer<>(createKafkaProducerProperties(requireNonNull(config, "config is null"))));
        if (resolveRetries.get() > 0) {
            log.warn("Had to retry to resolve bootstrap.urls: %s".formatted(config.getKafkaBootstrapServers()));
        }
    }

    private static Properties createKafkaProducerProperties(KafkaPublisherConfig config)
    {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Joiner.on(',').join(config.getKafkaBootstrapServers()));

        if (config.getKafkaSaslUsername().isPresent() && config.getKafkaSaslPassword().isPresent()) {
            properties.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule\n" +
                    "required username='%s'\n" +
                    "password='%s';\n",
                    config.getKafkaSaslUsername().get(),
                    config.getKafkaSaslPassword().get()));
            properties.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");
        }

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, config.getKafkaClientDisplayId());
        properties.put(ProducerConfig.ACKS_CONFIG, "all"); // Default: all

        // Note: the broker’s compression should also be set to “producer” in order to save the recompression cost,
        // in which case the broker appends the original compressed message sent by the producer.
        // Due to dependency resolution issues in trino for zstd-jni, zstd is not supported yet.
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // Default: none

        // The total bytes of memory the producer can use to buffer records waiting to be sent to the server.
        // If records are sent faster than they can be delivered to the server the producer will block for max.block.ms after which it will throw an exception.
        // Needs to be able to fit a single uncompressed message.
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"); // Default: 33554432
        // Max bytes for a single partition batch
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "2097152"); // Default: 16384
        // Max cumulative bytes for all partitions in a batch.
        // Needs to be able to fit a single uncompressed message.
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "16777216"); // Default: 1048576

        // Our error handling strategy is just to retry forever until any downstream issues are resolved (or until interrupt).
        // Since the calling thread needs to tolerate potential arbitrary blocking, there is no harm in just holding until the system recovers.
        // Also, this improves idempotency as future externally-driven retries can cause duplicates.

        // Max number of retries for a failed submission.
        properties.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE)); // Default: 2147483647
        // Max time to retry a failed submission. The value of this config should be greater than or equal to the sum of request.timeout.ms (30s) and linger.ms (0s).
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(Integer.MAX_VALUE)); // Default: 120000 (2 minutes)
        // The amount of time to block a caller waiting for metadata to initialize or buffer space to become available in the producer.
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Integer.MAX_VALUE)); // Default: 60000 (1 minute)

        // Just serialize everything as byte[] to force callers to handle serialization concerns
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return properties;
    }

    @Override
    public ListenableFuture<Void> publishPossiblyBlocking(ProducerRecord<byte[], byte[]> record)
            throws InterruptedException
    {
        SettableFuture<Void> settableFuture = SettableFuture.create();
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    settableFuture.set(null);
                }
                else {
                    settableFuture.setException(exception);
                }
            });
        }
        catch (InterruptException e) {
            throw new InterruptedException(e.getMessage());
        }
        catch (SerializationException e) {
            // Key or value are not valid objects given the configured serializers
            throw new AssertionError("SerializationException can not occur with bytes serialization", e);
        }
        catch (TimeoutException e) {
            // Record could not be appended to the send buffer due to memory unavailable or missing metadata within max.block.ms.
            throw new AssertionError("TimeoutException can not occur with infinite max.block.ms configuration", e);
        }
        catch (RuntimeException e) {
            settableFuture.setException(e);
        }
        return nonCancellationPropagating(settableFuture);
    }

    @Override
    public void flush()
            throws InterruptedException
    {
        try {
            producer.flush();
        }
        catch (InterruptException e) {
            throw new InterruptedException(e.getMessage());
        }
    }

    @Override
    public void close()
    {
        producer.close();
    }
}
