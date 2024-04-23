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

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class KafkaRecord
{
    private final String topic;
    private final byte[] payload;
    private Optional<Supplier<KafkaRecord>> fallbackSupplier;

    public KafkaRecord(String topic, byte[] payload)
    {
        this(topic, payload, Optional.empty());
    }

    public KafkaRecord(String topic, byte[] payload, Optional<Supplier<KafkaRecord>> fallbackSupplier)
    {
        this.topic = requireNonNull(topic, "topic is null");
        this.payload = requireNonNull(payload, "payload is null");
        this.fallbackSupplier = requireNonNull(fallbackSupplier, "fallbackSupplier is null");
    }

    public String getTopic()
    {
        return topic;
    }

    public byte[] getPayload()
    {
        return payload;
    }

    public Optional<Supplier<KafkaRecord>> getFallbackSupplier()
    {
        return fallbackSupplier;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaRecord record = (KafkaRecord) o;
        return topic.equals(record.topic) && Arrays.equals(payload, record.payload);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(topic);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
