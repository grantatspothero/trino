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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static com.google.common.util.concurrent.UncaughtExceptionHandlers.systemExit;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

@ThreadSafe
public class AsyncKafkaPublisher
{
    private static final Logger log = Logger.get(AsyncKafkaPublisher.class);

    private final String loggingName;
    private final int maxBufferingCapacity;
    private final BlockingDeque<KafkaRecord> buffer;
    private final KafkaPublisher kafkaPublisher;
    private final KafkaDeadLetter.Writer kafkaDeadLetterWriter;

    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicBoolean destroyed = new AtomicBoolean();
    private final AtomicBoolean errorDelay = new AtomicBoolean();
    private final RateLimiter errorRetryRateLimiter = RateLimiter.create(0.5);
    private final RateLimiter loggingRateLimiter = RateLimiter.create(0.5);
    private final ExecutorService executorService = newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("async-kafka-publisher")
            .setUncaughtExceptionHandler(systemExit())
            .build());
    private final ExecutorService deadLetterWriterExecutor = newSingleThreadExecutor(daemonThreadsNamed("dead-letter-writer"));

    private final DistributionStat payloadByteSize = new DistributionStat();
    private final CounterStat publishedRecords = new CounterStat();
    private final CounterStat publishedPayloadBytes = new CounterStat();
    private final CounterStat failedRecordErrorRetries = new CounterStat();
    private final CounterStat queueOverflowDroppedRecords = new CounterStat();
    private final CounterStat payloadTooLargeDroppedRecords = new CounterStat();

    public AsyncKafkaPublisher(String loggingName, int maxBufferingCapacity, KafkaPublisherConfig config)
    {
        this(loggingName, maxBufferingCapacity, new KafkaPublisherClient(config), KafkaDeadLetter.create(config));
    }

    @VisibleForTesting
    AsyncKafkaPublisher(String loggingName, int maxBufferingCapacity, KafkaPublisher kafkaPublisher)
    {
        this(loggingName, maxBufferingCapacity, kafkaPublisher, KafkaDeadLetter.NO_OP_WRITER);
    }

    private AsyncKafkaPublisher(String loggingName, int maxBufferingCapacity, KafkaPublisher kafkaPublisher, KafkaDeadLetter.Writer kafkaDeadLetterWriter)
    {
        this.loggingName = requireNonNull(loggingName, "loggingName is null");
        checkArgument(maxBufferingCapacity > 0, "maxBufferingCapacity must be positive");
        this.maxBufferingCapacity = maxBufferingCapacity;
        this.buffer = new LinkedBlockingDeque<>(maxBufferingCapacity);
        this.kafkaPublisher = requireNonNull(kafkaPublisher, "kafkaPublisher is null");
        this.kafkaDeadLetterWriter = requireNonNull(kafkaDeadLetterWriter, "kafkaDeadLetterWriter is null");
    }

    public void initialize()
    {
        if (!initialized.compareAndSet(false, true)) {
            return;
        }

        log.info("%s - Initializing", loggingName);
        executorService.execute(() -> {
            log.info("%s - Starting draining thread", loggingName);
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        if (errorDelay.getAndSet(false)) {
                            // Throttle processing if the last result was an error of some sort
                            errorRetryRateLimiter.acquire();
                        }
                        handleRecord(buffer.take());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.info("%s - Buffer draining interrupted", loggingName);
                    }
                    catch (RuntimeException e) {
                        log.error("%s - Unexpected error draining buffer. Will continue after a short delay...", loggingName);
                        errorDelay.set(true);
                    }
                }
            }
            finally {
                log.info("%s - Draining thread stopped", loggingName);
            }
        });
        log.info("%s - Initialized", loggingName);
    }

    public void destroy()
    {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }

        log.info("%s - Destroying", loggingName);
        try (KafkaPublisher publisher = kafkaPublisher) {
            shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
        }
        deadLetterWriterExecutor.shutdownNow();
        log.info("%s - Destroyed", loggingName);
    }

    public void submit(KafkaRecord record)
    {
        payloadByteSize.add(record.getPayload().length);

        while (!buffer.offerLast(record)) {
            // Drop older records in favor of newer records if backed up to capacity limits
            if (buffer.pollFirst() != null) {
                queueOverflowDroppedRecords.update(1);
            }
            if (loggingRateLimiter.tryAcquire()) {
                log.error("%s - record dropped! Async queue has reached max buffering capacity of %s", loggingName, maxBufferingCapacity);
            }
        }
    }

    private void handleRecord(KafkaRecord record)
    {
        try {
            ListenableFuture<Void> future = kafkaPublisher.publishPossiblyBlocking(new ProducerRecord<>(record.getTopic(), record.getPayload()));
            Futures.addCallback(
                    future,
                    new FutureCallback<>()
                    {
                        @Override
                        public void onSuccess(Void result)
                        {
                            publishedRecords.update(1);
                            publishedPayloadBytes.update(record.getPayload().length);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            if (t instanceof RecordTooLargeException) {
                                log.error(t, "%s - Raw payload size of %s bytes was too large to publish. Dropping record!", loggingName, record.getPayload().length);
                                payloadTooLargeDroppedRecords.update(1);
                                deadLetterWriterExecutor.execute(() -> {
                                    try {
                                        kafkaDeadLetterWriter.write(record.getTopic(), record.getPayload());
                                    }
                                    catch (Throwable t2) {
                                        log.error(t2, "%s - Failed to write Kafka dead letter", loggingName);
                                    }
                                });
                            }
                            else {
                                log.error(t, "%s - Failed to publish record. Will reattempt after a short delay", loggingName);
                                requeueFailedRecord(record);
                            }
                        }
                    },
                    directExecutor());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn(e, "%s - Interrupted publishing record. Shutting down?", loggingName);
            requeueFailedRecord(record);
        }
        catch (RuntimeException e) {
            log.error(e, "%s - Unexpected publish record failure. Will reattempt after a short delay", loggingName);
            requeueFailedRecord(record);
        }
    }

    private void requeueFailedRecord(KafkaRecord record)
    {
        failedRecordErrorRetries.update(1);
        errorDelay.set(true);

        // Requeue record at the front of the line
        if (!buffer.offerFirst(record)) {
            // Buffer is at capacity, so just drop it because we give preference to newer records
            queueOverflowDroppedRecords.update(1);
            log.warn("%s - Failed to enqueue retry record. Async queue has reached max buffering capacity of %s", loggingName, maxBufferingCapacity);
        }
    }

    public int getQueueSize()
    {
        return buffer.size();
    }

    public boolean isCapacitySaturated()
    {
        return buffer.remainingCapacity() == 0;
    }

    public DistributionStat getPayloadByteSize()
    {
        return payloadByteSize;
    }

    public CounterStat getPublishedRecords()
    {
        return publishedRecords;
    }

    public CounterStat getPublishedPayloadBytes()
    {
        return publishedPayloadBytes;
    }

    public CounterStat getFailedRecordErrorRetries()
    {
        return failedRecordErrorRetries;
    }

    public CounterStat getPayloadTooLargeDroppedRecords()
    {
        return payloadTooLargeDroppedRecords;
    }

    public CounterStat getQueueOverflowDroppedRecords()
    {
        return queueOverflowDroppedRecords;
    }
}
