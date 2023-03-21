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
import io.airlift.units.Duration;
import org.assertj.core.api.Condition;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.trino.galaxy.kafka.KafkaPublisherClient.UNABLE_TO_RESOLVE_PREDICATE;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKafkaPublisherClient
{
    @Test
    public void verifyThrownWhenResolutionImpossible()
    {
        assertThatThrownBy(() -> new KafkaPublisherClient(new KafkaPublisherConfig()
                .setKafkaBootstrapServersResolutionTimeout(new Duration(10, TimeUnit.SECONDS))
                .setKafkaBootstrapServers(ImmutableList.of(UUID.randomUUID() + ".com:9092"))
                .setKafkaClientDisplayId("testing-client-id-" + UUID.randomUUID())))
                .has(new Condition<>(UNABLE_TO_RESOLVE_PREDICATE, "Config error should still be in the stack trace on failure"));
    }

    @Test(timeOut = 10_000)
    public void verifyOtherErrorsThrown()
    {
        assertThatThrownBy(() -> new KafkaPublisherClient(new KafkaPublisherConfig()
                .setKafkaBootstrapServersResolutionTimeout(new Duration(60, TimeUnit.SECONDS))
                .setKafkaBootstrapServers(ImmutableList.of("starburst.io")) //something resolvable but wrong
                .setKafkaClientDisplayId("testing-client-id-" + UUID.randomUUID())))
                .has(new Condition<>(not(UNABLE_TO_RESOLVE_PREDICATE), "Config error should not be in the stack, and not retried"));
    }
}
