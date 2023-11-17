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
package io.trino.filesystem.s3;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logging;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.galaxy.CatalogNetworkMonitor;
import io.trino.spi.security.ConnectorIdentity;
import org.assertj.core.data.Offset;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
public class TestS3FileSystemNetworkTracking
{
    private static final int BYTES_IN_MEGABYTE = 1024 * 1024;
    private static final DataSize HIGH_CROSS_REGION_LIMIT = DataSize.of(1, Unit.GIGABYTE);
    private static final DataSize ZERO_CROSS_REGION_LIMIT = DataSize.ofBytes(0);
    private static final ConnectorIdentity TESTING_CONNECTOR_IDENTITY = ConnectorIdentity.ofUser("test");

    private final String accessKey = environmentVariable("AWS_ACCESS_KEY_ID");
    private final String secretKey = environmentVariable("AWS_SECRET_ACCESS_KEY");
    private final String region = environmentVariable("AWS_REGION");
    private final String bucket = environmentVariable("S3_BUCKET");

    @BeforeAll
    public void init()
    {
        Logging.initialize();
    }

    @Test
    @Order(1)
    public void testS3FileSystemEnforcesCrossRegionWriteLimits()
            throws IOException
    {
        String catalogName = newCatalogName();
        String catalogId = createCatalogId(catalogName);
        S3FileSystemFactory fileSystemFactory = createS3FileSystemFactory(catalogId, HIGH_CROSS_REGION_LIMIT, ZERO_CROSS_REGION_LIMIT);
        TrinoFileSystem fileSystem = fileSystemFactory.create(TESTING_CONNECTOR_IDENTITY);
        Location tempBlobLocation = createLocation("writeLimitEnforcing/%s".formatted(UUID.randomUUID()));
        CatalogNetworkMonitor catalogNetworkMonitor = CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, HIGH_CROSS_REGION_LIMIT.toBytes(), ZERO_CROSS_REGION_LIMIT.toBytes());

        try {
            TrinoOutputFile outputFile = fileSystem.newOutputFile(tempBlobLocation);
            OutputStream outputStream = outputFile.create();
            byte[] bytes = Slices.random(4).byteArray();
            outputStream.write(bytes);
            assertThatThrownBy(outputStream::close).isInstanceOf(TrinoException.class).hasMessage("Cross-region write data transfer limit of 0GB per worker exceeded. To increase this limit, contact Starburst support.");

            // It is expected that a small amount of bytes was transferred
            // TODO: Check limits before writing https://github.com/starburstdata/galaxy-trino/issues/1259
            assertThat(catalogNetworkMonitor.getCrossRegionWriteBytes()).isCloseTo(0, Offset.offset(2000L));
        }
        finally {
            fileSystemFactory.destroy();
            try (S3Client s3Client = createS3Client()) {
                DeleteObjectRequest request = DeleteObjectRequest.builder()
                        .key(new S3Location(tempBlobLocation).key())
                        .bucket(bucket)
                        .build();
                s3Client.deleteObject(request);
            }
        }
    }

    @Test
    @Order(2)
    public void testS3FileSystemEnforcesCrossRegionReadLimits()
            throws IOException
    {
        String catalogName = newCatalogName();
        String catalogId = createCatalogId(catalogName);
        S3FileSystemFactory fileSystemFactory = createS3FileSystemFactory(catalogId, ZERO_CROSS_REGION_LIMIT, HIGH_CROSS_REGION_LIMIT);
        TrinoFileSystem fileSystem = fileSystemFactory.create(TESTING_CONNECTOR_IDENTITY);
        Location tempBlobLocation = createLocation("readLimitEnforcing/%s".formatted(UUID.randomUUID()));
        CatalogNetworkMonitor catalogNetworkMonitor = CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, ZERO_CROSS_REGION_LIMIT.toBytes(), HIGH_CROSS_REGION_LIMIT.toBytes());

        try {
            TrinoInputFile inputFile = fileSystem.newInputFile(tempBlobLocation);
            TrinoInputStream inputStream = inputFile.newStream();
            assertThatThrownBy(inputStream::read).isInstanceOf(TrinoException.class).hasMessage("Cross-region read data transfer limit of 0GB per worker exceeded. To increase this limit, contact Starburst support.");
            inputStream.close();

            // It is expected that a small amount of bytes was transferred
            assertThat(catalogNetworkMonitor.getCrossRegionReadBytes()).isCloseTo(0, Offset.offset(500L));
        }
        finally {
            fileSystemFactory.destroy();
            try (S3Client s3Client = createS3Client()) {
                DeleteObjectRequest request = DeleteObjectRequest.builder()
                        .key(new S3Location(tempBlobLocation).key())
                        .bucket(bucket)
                        .build();
                s3Client.deleteObject(request);
            }
        }
    }

    @Test
    @Order(3)
    public void testS3FileSystemAccurateNetworkTracking()
            throws IOException
    {
        String catalogName = newCatalogName();
        String catalogId = createCatalogId(catalogName);
        S3FileSystemFactory fileSystemFactory = createS3FileSystemFactory(catalogId, HIGH_CROSS_REGION_LIMIT, HIGH_CROSS_REGION_LIMIT);
        TrinoFileSystem fileSystem = fileSystemFactory.create(TESTING_CONNECTOR_IDENTITY);
        Location tempBlobLocation = createLocation("networkTracking/%s".formatted(UUID.randomUUID()));
        CatalogNetworkMonitor catalogNetworkMonitor = CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, HIGH_CROSS_REGION_LIMIT.toBytes(), HIGH_CROSS_REGION_LIMIT.toBytes());

        try {
            // write a 16 MB file
            TrinoOutputFile outputFile = fileSystem.newOutputFile(tempBlobLocation);
            long previousWriteBytes;
            try (OutputStream outputStream = outputFile.create()) {
                previousWriteBytes = catalogNetworkMonitor.getCrossRegionWriteBytes();
                byte[] bytes = new byte[4];
                Slice slice = Slices.wrappedBuffer(bytes);
                for (int i = 0; i < 4 * BYTES_IN_MEGABYTE; i++) {
                    slice.setInt(0, i);
                    outputStream.write(bytes);
                }
            }

            // check that network monitor accurately tracked the bytes written with minimal difference
            long expectedWriteBytes = catalogNetworkMonitor.getCrossRegionWriteBytes() - previousWriteBytes;
            assertThat(expectedWriteBytes).isCloseTo(16 * BYTES_IN_MEGABYTE, Percentage.withPercentage(0.1));

            // read the 16 MB file in
            TrinoInputFile inputFile = fileSystem.newInputFile(tempBlobLocation);
            long previousReadBytes;
            try (TrinoInputStream inputStream = inputFile.newStream()) {
                previousReadBytes = catalogNetworkMonitor.getCrossRegionReadBytes();
                byte[] bytes = new byte[4];
                Slice slice = Slices.wrappedBuffer(bytes);
                for (int intPosition = 0; intPosition < 4 * BYTES_IN_MEGABYTE; intPosition++) {
                    slice.setInt(0, intPosition);
                    for (byte b : bytes) {
                        int value = inputStream.read();
                        assertThat(value).isGreaterThanOrEqualTo(0);
                        assertThat((byte) value).isEqualTo(b);
                    }
                }
            }

            // check that network monitor accurately tracked the bytes read with minimal difference
            long expectedReadBytes = catalogNetworkMonitor.getCrossRegionReadBytes() - previousReadBytes;
            assertThat(expectedReadBytes).isCloseTo(16 * BYTES_IN_MEGABYTE, Percentage.withPercentage(0.1));
        }
        finally {
            fileSystemFactory.destroy();
            try (S3Client s3Client = createS3Client()) {
                DeleteObjectRequest request = DeleteObjectRequest.builder()
                        .key(new S3Location(tempBlobLocation).key())
                        .bucket(bucket)
                        .build();
                s3Client.deleteObject(request);
            }
        }
    }

    private S3Client createS3Client()
    {
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.of(region))
                .build();
    }

    private S3FileSystemFactory createS3FileSystemFactory(String catalogId, DataSize crossRegionReadLimit, DataSize crossRegionWriteLimit)
    {
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey(accessKey)
                        .setAwsSecretKey(secretKey).setRegion(region)
                        .setStreamingPartSize(DataSize.valueOf("5.5MB")),
                CatalogHandle.fromId(catalogId),
                new LocalRegionConfig()
                        // forces network traffic to be cross-region
                        .setAllowedIpAddresses(ImmutableList.of("0.0.0.0")),
                new CrossRegionConfig()
                        .setAllowCrossRegionAccess(true)
                        .setCrossRegionReadLimit(crossRegionReadLimit)
                        .setCrossRegionWriteLimit(crossRegionWriteLimit));
    }

    private String newCatalogName()
    {
        return "cross-region-catalog-%s".formatted(ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE));
    }

    private String createCatalogId(String catalogName)
    {
        return "%s:normal:1".formatted(catalogName);
    }

    private Location createLocation(String path)
    {
        return getRootLocation().appendPath(path);
    }

    private Location getRootLocation()
    {
        return Location.of("s3://%s/".formatted(bucket));
    }

    private static String environmentVariable(String name)
    {
        return requireNonNull(System.getenv(name), "Environment variable not set: " + name);
    }
}
