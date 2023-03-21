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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestKafkaPublisherConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KafkaPublisherConfig.class)
                .setKafkaBootstrapServers(null)
                .setKafkaSaslUsername(null)
                .setKafkaSaslPassword(null)
                .setKafkaClientDisplayId(null)
                .setKafkaDeadLetterS3Bucket(null)
                .setKafkaDeadLetterS3Prefix(null)
                .setKafkaBootstrapServersResolutionTimeout(new Duration(2, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kafka.bootstrap.servers", "example.com:9092,localhost:9092")
                .put("kafka.sasl.username", "user")
                .put("kafka.sasl.password", "password")
                .put("kafka.client.display-id", "my-client")
                .put("kafka.dead-letter.s3-bucket", "my-bucket")
                .put("kafka.dead-letter.s3-prefix", "my-prefix")
                .put("kafka.bootstrap.servers.resolution.timeout", "10m")
                .buildOrThrow();

        KafkaPublisherConfig expected = new KafkaPublisherConfig()
                .setKafkaBootstrapServers(ImmutableList.of("example.com:9092", "localhost:9092"))
                .setKafkaSaslUsername("user")
                .setKafkaSaslPassword("password")
                .setKafkaClientDisplayId("my-client")
                .setKafkaDeadLetterS3Bucket("my-bucket")
                .setKafkaDeadLetterS3Prefix("my-prefix")
                .setKafkaBootstrapServersResolutionTimeout(new Duration(10, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }
}
