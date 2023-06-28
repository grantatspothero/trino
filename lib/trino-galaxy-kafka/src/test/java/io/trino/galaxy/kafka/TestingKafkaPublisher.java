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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

@ThreadSafe
public class TestingKafkaPublisher
        implements KafkaPublisher
{
    private final AtomicReference<CountDownLatch> blockedSubmissionLatch = new AtomicReference<>();
    private final AtomicReference<ListenableFuture<Void>> futureState = new AtomicReference<>(immediateFuture(null));

    private final List<KafkaRecord> attemptedRecords = new CopyOnWriteArrayList<>();
    private final List<KafkaRecord> publishedRecords = new CopyOnWriteArrayList<>();

    @Override
    public ListenableFuture<Void> publishPossiblyBlocking(ProducerRecord<byte[], byte[]> record)
    {
        synchronized (attemptedRecords) {
            attemptedRecords.add(toRecord(record));
            attemptedRecords.notifyAll();
        }

        CountDownLatch latch = blockedSubmissionLatch.get();
        if (latch != null) {
            Uninterruptibles.awaitUninterruptibly(latch);
        }

        ListenableFuture<Void> future = futureState.get();
        // Use transform to ensure side-effects are realized before the caller can observe the successful result.
        return transform(
                future,
                ignored -> {
                    synchronized (publishedRecords) {
                        publishedRecords.add(toRecord(record));
                        publishedRecords.notifyAll();
                    }
                    return null;
                },
                directExecutor());
    }

    @Override
    public void flush()
            throws InterruptedException
    {
        try {
            futureState.get().get();
        }
        catch (ExecutionException ignored) {
        }
    }

    public List<KafkaRecord> getAttemptedRecords()
    {
        return attemptedRecords;
    }

    public List<KafkaRecord> getPublishedRecords()
    {
        return publishedRecords;
    }

    public void awaitAttempt(Predicate<List<KafkaRecord>> predicate, long timeoutMs)
            throws InterruptedException, TimeoutException
    {
        awaitInternal(attemptedRecords, predicate, timeoutMs);
    }

    public void awaitPublish(Predicate<List<KafkaRecord>> predicate, long timeoutMs)
            throws InterruptedException, TimeoutException
    {
        awaitInternal(publishedRecords, predicate, timeoutMs);
    }

    private static void awaitInternal(List<KafkaRecord> list, Predicate<List<KafkaRecord>> predicate, long timeoutMs)
            throws InterruptedException, TimeoutException
    {
        long startNs = System.nanoTime();
        synchronized (list) {
            while (!predicate.test(list)) {
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
                if (elapsedMs >= timeoutMs) {
                    throw new TimeoutException("Timed out waiting for condition");
                }
                list.wait(timeoutMs - elapsedMs);
            }
        }
    }

    public void setBlockedSubmissionLatch(CountDownLatch latch)
    {
        blockedSubmissionLatch.set(latch);
    }

    public void setFutureState(ListenableFuture<Void> future)
    {
        futureState.set(future);
    }

    private static KafkaRecord toRecord(ProducerRecord<byte[], byte[]> record)
    {
        return new KafkaRecord(record.topic(), record.value());
    }
}
