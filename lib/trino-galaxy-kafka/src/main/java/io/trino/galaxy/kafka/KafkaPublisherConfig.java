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
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;

public class KafkaPublisherConfig
{
    private List<String> kafkaBootstrapServers;
    private Optional<String> kafkaSaslUsername = Optional.empty();
    private Optional<String> kafkaSaslPassword = Optional.empty();
    private String kafkaClientDisplayId;
    private Optional<String> kafkaDeadLetterS3Bucket = Optional.empty();
    private Optional<String> kafkaDeadLetterS3Prefix = Optional.empty();

    @NotEmpty
    public List<String> getKafkaBootstrapServers()
    {
        return kafkaBootstrapServers;
    }

    @Config("kafka.bootstrap.servers")
    @ConfigDescription("Comma-separated list of host:ports to bootstrap the Kafka client. Can also pass DNS A records with multiple IPs. See Kafka 'bootstrap.servers' config.")
    public KafkaPublisherConfig setKafkaBootstrapServers(List<String> kafkaBootstrapServers)
    {
        this.kafkaBootstrapServers = kafkaBootstrapServers == null ? null : ImmutableList.copyOf(kafkaBootstrapServers);
        return this;
    }

    @NotNull
    public Optional<String> getKafkaSaslUsername()
    {
        return kafkaSaslUsername;
    }

    @Config("kafka.sasl.username")
    @ConfigDescription("Kafka SASL username (if used)")
    public KafkaPublisherConfig setKafkaSaslUsername(String kafkaSaslUsername)
    {
        this.kafkaSaslUsername = Optional.ofNullable(kafkaSaslUsername);
        return this;
    }

    @NotNull
    public Optional<String> getKafkaSaslPassword()
    {
        return kafkaSaslPassword;
    }

    @Config("kafka.sasl.password")
    @ConfigDescription("Kafka SASL password (if used)")
    @ConfigSecuritySensitive
    public KafkaPublisherConfig setKafkaSaslPassword(String kafkaSaslPassword)
    {
        this.kafkaSaslPassword = Optional.ofNullable(kafkaSaslPassword);
        return this;
    }

    @NotEmpty
    public String getKafkaClientDisplayId()
    {
        return kafkaClientDisplayId;
    }

    @Config("kafka.client.display-id")
    @ConfigDescription("Client ID that Kafka will use for reporting client activity")
    public KafkaPublisherConfig setKafkaClientDisplayId(String kafkaClientDisplayId)
    {
        this.kafkaClientDisplayId = kafkaClientDisplayId;
        return this;
    }

    @NotNull
    public Optional<String> getKafkaDeadLetterS3Bucket()
    {
        return kafkaDeadLetterS3Bucket;
    }

    @Config("kafka.dead-letter.s3-bucket")
    @ConfigDescription("S3 bucket for dropped messages")
    public KafkaPublisherConfig setKafkaDeadLetterS3Bucket(String kafkaDeadLetterS3Bucket)
    {
        this.kafkaDeadLetterS3Bucket = Optional.ofNullable(kafkaDeadLetterS3Bucket);
        return this;
    }

    @NotNull
    public Optional<String> getKafkaDeadLetterS3Prefix()
    {
        return kafkaDeadLetterS3Prefix;
    }

    @Config("kafka.dead-letter.s3-prefix")
    @ConfigDescription("S3 prefix for dropped messages")
    public KafkaPublisherConfig setKafkaDeadLetterS3Prefix(String kafkaDeadLetterS3Prefix)
    {
        this.kafkaDeadLetterS3Prefix = Optional.ofNullable(kafkaDeadLetterS3Prefix);
        return this;
    }

    @AssertTrue(message = "Kafka SASL username and password must be provided together")
    public boolean isKafkaSaslUsernameAndPasswordAligned()
    {
        return kafkaSaslUsername.isPresent() == kafkaSaslPassword.isPresent();
    }

    @AssertTrue(message = "Kafka dead letter bucket and prefix must be provided together")
    public boolean isKafkaDeadLetterBucketAndPrefixAligned()
    {
        return kafkaDeadLetterS3Bucket.isPresent() == kafkaDeadLetterS3Prefix.isPresent();
    }
}
