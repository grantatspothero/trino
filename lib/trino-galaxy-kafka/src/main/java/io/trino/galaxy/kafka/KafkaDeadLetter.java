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

import com.google.common.util.concurrent.RateLimiter;
import io.airlift.log.Logger;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ROLE_ARN;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_WEB_IDENTITY_TOKEN_FILE;

public final class KafkaDeadLetter
{
    private static final Logger log = Logger.get(KafkaDeadLetter.class);
    public static final Writer NO_OP_WRITER = (topic, payload) -> {};

    private KafkaDeadLetter() {}

    public interface Writer
    {
        void write(String topic, byte[] payload);
    }

    public static KafkaDeadLetter.Writer create(KafkaPublisherConfig config)
    {
        if (config.getKafkaDeadLetterS3Bucket().isEmpty()) {
            return NO_OP_WRITER;
        }
        return new S3KafkaDeadLetterWriter(
                config.getKafkaDeadLetterS3Bucket().get(),
                config.getKafkaDeadLetterS3Prefix().orElseThrow(IllegalStateException::new));
    }

    private static class S3KafkaDeadLetterWriter
            implements Writer
    {
        private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
                .appendValue(YEAR, 4)
                .appendValue(MONTH_OF_YEAR, 2)
                .appendValue(DAY_OF_MONTH, 2)
                .toFormatter();
        private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
                .appendValue(HOUR_OF_DAY, 2)
                .appendValue(MINUTE_OF_HOUR, 2)
                .appendValue(SECOND_OF_MINUTE, 2)
                .toFormatter();
        private final S3Client s3Client;
        private final String bucket;
        private final String prefix;
        private final RateLimiter bytesRateLimiter = RateLimiter.create(10_000.0); // 10K * 3600 ~= 34MB / hour

        public S3KafkaDeadLetterWriter(String bucket, String prefix)
        {
            StsAssumeRoleWithWebIdentityCredentialsProvider provider;
            try {
                provider = StsAssumeRoleWithWebIdentityCredentialsProvider.builder()
                        .stsClient(StsClient.create())
                        .refreshRequest(AssumeRoleWithWebIdentityRequest.builder()
                                .roleArn(AWS_ROLE_ARN.getStringValueOrThrow())
                                .roleSessionName("galaxy-kafka-dead-letter")
                                .webIdentityToken(Files.readString(Paths.get(AWS_WEB_IDENTITY_TOKEN_FILE.getStringValueOrThrow())))
                                .build())
                        .build();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            // Most dropped messages are from eu-west1
            this.s3Client = S3Client.builder()
                    .region(Region.EU_WEST_1)
                    .credentialsProvider(provider)
                    .build();
            this.bucket = requireNonNull(bucket, "bucket is null");
            this.prefix = requireNonNull(prefix, "prefix is null");
        }

        @Override
        public void write(String topic, byte[] payload)
        {
            if (!bytesRateLimiter.tryAcquire(payload.length)) {
                log.warn("Dead letter dropped from Kafka topic: %s! Rate limit reached", topic);
                return;
            }
            OffsetDateTime dateTime = Instant.now().atOffset(ZoneOffset.UTC);
            String date = DATE_FORMATTER.format(dateTime);
            String time = TIME_FORMATTER.format(dateTime);
            String filename = "%s-%s-%s.gz".formatted(date, time, UUID.randomUUID().toString());
            String key = String.join("/", prefix, topic, date, filename);

            try (ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
                    GZIPOutputStream gzipOutput = new GZIPOutputStream(byteArrayOutput)) {
                gzipOutput.write(payload);
                gzipOutput.finish();

                byte[] bytes = byteArrayOutput.toByteArray();
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentLength((long) bytes.length)
                        .build();
                s3Client.putObject(request, RequestBody.fromBytes(bytes));
                log.info("Dead letter written to s3://%s/%s, length = %d bytes", bucket, key, bytes.length);
            }
            catch (IOException e) {
                log.error(e, "Failed to write dead letter from Kafka topic: %s", topic);
            }
        }
    }
}
