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

import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAsyncKafkaPublisher
{
    @Test
    public void testBasicSubmission()
            throws InterruptedException, TimeoutException
    {
        TestingKafkaPublisher testingPublisher = new TestingKafkaPublisher();
        AsyncKafkaPublisher asyncKafkaPublisher = new AsyncKafkaPublisher("test", 10, testingPublisher);
        asyncKafkaPublisher.initialize();

        KafkaRecord record1 = newRecord("topic", "1");
        asyncKafkaPublisher.submit(record1);
        testingPublisher.awaitPublish(records -> records.contains(record1), 5000);
        assertThat(testingPublisher.getPublishedRecords()).containsExactlyInAnyOrder(record1);
        assertThat(asyncKafkaPublisher.isCapacitySaturated()).isFalse();

        KafkaRecord record2 = newRecord("topic2", "2");
        asyncKafkaPublisher.submit(record2);
        testingPublisher.awaitPublish(records -> records.contains(record2), 5000);
        assertThat(testingPublisher.getPublishedRecords()).containsExactlyInAnyOrder(record1, record2);
        assertThat(asyncKafkaPublisher.isCapacitySaturated()).isFalse();

        KafkaRecord record3 = newRecord("topic3", "3");
        asyncKafkaPublisher.submit(record3);
        testingPublisher.awaitPublish(records -> records.contains(record3), 5000);
        assertThat(testingPublisher.getPublishedRecords()).containsExactlyInAnyOrder(record1, record2, record3);
        assertThat(asyncKafkaPublisher.isCapacitySaturated()).isFalse();
    }

    @Test
    public void testCapacityOverflow()
            throws InterruptedException, TimeoutException
    {
        TestingKafkaPublisher testingPublisher = new TestingKafkaPublisher();
        AsyncKafkaPublisher asyncKafkaPublisher = new AsyncKafkaPublisher("test", 2, testingPublisher);
        asyncKafkaPublisher.initialize();

        // Establish a blocking client
        CountDownLatch latch = new CountDownLatch(1);
        testingPublisher.setBlockedSubmissionLatch(latch);

        // We should be able to submit maxBufferingCapacity+1 records before capacity is saturated

        // Submit the first record and it should be extracted for processing, but remain blocked on the client
        KafkaRecord record1 = newRecord("topic", "1");
        asyncKafkaPublisher.submit(record1);
        testingPublisher.awaitAttempt(records -> records.contains(record1), 5000);
        assertThat(testingPublisher.getPublishedRecords()).isEmpty();
        assertThat(asyncKafkaPublisher.isCapacitySaturated()).isFalse();

        // The next two records will cause the kafka producer to reach capacity
        KafkaRecord record2 = newRecord("topic", "2");
        asyncKafkaPublisher.submit(record2);
        assertThat(testingPublisher.getPublishedRecords()).isEmpty();
        assertThat(asyncKafkaPublisher.isCapacitySaturated()).isFalse();
        KafkaRecord record3 = newRecord("topic", "3");
        asyncKafkaPublisher.submit(record3);
        assertThat(testingPublisher.getPublishedRecords()).isEmpty();
        assertThat(asyncKafkaPublisher.isCapacitySaturated()).isTrue();

        // The next record should cause the oldest buffered record (record2) to be dropped
        KafkaRecord record4 = newRecord("topic", "4");
        asyncKafkaPublisher.submit(record4);

        // Unblock the client
        latch.countDown();

        testingPublisher.awaitPublish(records -> records.size() == 3, 5000);
        assertThat(testingPublisher.getPublishedRecords()).containsExactlyInAnyOrder(record1, record3, record4);
    }

    @Test
    public void testFailureRetry()
            throws InterruptedException, TimeoutException
    {
        TestingKafkaPublisher testingPublisher = new TestingKafkaPublisher();
        AsyncKafkaPublisher asyncKafkaPublisher = new AsyncKafkaPublisher("test", 2, testingPublisher);
        asyncKafkaPublisher.initialize();

        // Establish a failing client
        testingPublisher.setFutureState(immediateFailedFuture(new RuntimeException()));

        // Submit an record that should keep retrying repeatedly
        KafkaRecord record1 = newRecord("topic", "1");
        asyncKafkaPublisher.submit(record1);
        // Await 3 retries
        testingPublisher.awaitAttempt(records -> records.size() > 3, 5000);

        // Establish a succeeding client
        testingPublisher.setFutureState(immediateFuture(null));
        testingPublisher.awaitPublish(records -> records.contains(record1), 5000);
        assertThat(testingPublisher.getPublishedRecords()).containsExactlyInAnyOrder(record1);
    }

    @Test
    public void testFailureRetryCapacityOverflow()
            throws InterruptedException, TimeoutException
    {
        TestingKafkaPublisher testingPublisher = new TestingKafkaPublisher();
        AsyncKafkaPublisher asyncKafkaPublisher = new AsyncKafkaPublisher("test", 2, testingPublisher);
        asyncKafkaPublisher.initialize();

        // Establish a blocking client
        CountDownLatch latch = new CountDownLatch(1);
        testingPublisher.setBlockedSubmissionLatch(latch);

        // Submit the first record and it should be extracted for processing, but remain blocked on the client
        KafkaRecord record1 = newRecord("topic", "1");
        asyncKafkaPublisher.submit(record1);
        testingPublisher.awaitAttempt(records -> records.contains(record1), 5000);
        assertThat(testingPublisher.getPublishedRecords()).isEmpty();
        assertThat(asyncKafkaPublisher.isCapacitySaturated()).isFalse();

        // The next two records will cause the kafka producer to reach capacity
        KafkaRecord record2 = newRecord("topic", "2");
        asyncKafkaPublisher.submit(record2);
        assertThat(testingPublisher.getPublishedRecords()).isEmpty();
        assertThat(asyncKafkaPublisher.isCapacitySaturated()).isFalse();
        KafkaRecord record3 = newRecord("topic", "3");
        asyncKafkaPublisher.submit(record3);
        assertThat(testingPublisher.getPublishedRecords()).isEmpty();
        assertThat(asyncKafkaPublisher.isCapacitySaturated()).isTrue();

        // Establish a failing client and unblock
        testingPublisher.setFutureState(immediateFailedFuture(new RuntimeException()));
        latch.countDown();

        // Await 3 publish attempts of any records
        testingPublisher.awaitAttempt(records -> records.size() > 3, 5000);

        // Establish a succeeding client
        testingPublisher.setFutureState(immediateFuture(null));

        // The first record (record1) should have been dropped since it was the oldest
        testingPublisher.awaitPublish(records -> records.containsAll(ImmutableList.of(record2, record3)), 5000);
        assertThat(testingPublisher.getPublishedRecords()).containsExactlyInAnyOrder(record2, record3);
    }

    @Test
    public void testFailureFallback()
            throws InterruptedException, TimeoutException
    {
        TestingKafkaPublisher testingPublisher = new TestingKafkaPublisher();
        AsyncKafkaPublisher asyncKafkaPublisher = new AsyncKafkaPublisher("test", 2, testingPublisher);
        asyncKafkaPublisher.initialize();

        // Establish a RecordTooLargeException client
        testingPublisher.setFutureState(immediateFailedFuture(new RecordTooLargeException()));

        // Submit a record that falls back to a different record twice
        KafkaRecord record3 = new KafkaRecord("topic", "3".getBytes(UTF_8));
        KafkaRecord record2 = new KafkaRecord("topic", "2".getBytes(UTF_8), Optional.of(() -> record3));
        KafkaRecord record1 = new KafkaRecord("topic", "1".getBytes(UTF_8), Optional.of(() -> record2));
        asyncKafkaPublisher.submit(record1);

        // Await for one retry to trigger fallback
        testingPublisher.awaitAttempt(records -> records.size() == 2, 5000);

        // Establish a succeeding client
        testingPublisher.setFutureState(immediateFuture(null));

        testingPublisher.awaitPublish(records -> records.contains(record3), 5000);
        assertThat(testingPublisher.getPublishedRecords()).containsExactlyInAnyOrder(record3);
    }

    private static KafkaRecord newRecord(String topic, String value)
    {
        return new KafkaRecord(topic, value.getBytes(UTF_8));
    }

    private static KafkaRecord toRecord(ProducerRecord<byte[], byte[]> record)
    {
        return new KafkaRecord(record.topic(), record.value());
    }
}
