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
import org.apache.kafka.clients.producer.ProducerRecord;

public interface KafkaPublisher
        extends AutoCloseable
{
    /**
     * Publish a Kafka record, with the future result reported in the returned ListenableFuture. This call may block for an arbitrary amount of time if the
     * internal client has saturated buffer capacity. There is no reason to try to parallelize around this call because the internal implementation
     * already performs multiple concurrent publishing operations.
     * <p>
     * Note: may complete out of order.
     */
    ListenableFuture<Void> publishPossiblyBlocking(ProducerRecord<byte[], byte[]> record)
            throws InterruptedException;

    /**
     * Forces all previous in-flight records to be flushed out to Kafka. Guarantees that all previously returned ListenableFuture's will be in a done
     * state.
     */
    void flush()
            throws InterruptedException;

    @Override
    default void close() {}
}
