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
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(Lifecycle.PER_CLASS)
public class TestS3FileSystemNetworkTracking
{
    private static final int BYTES_IN_MEGABYTE = 1024 * 1024;
    private static final String CATALOG_NAME = "catalog";
    private static final String CATALOG_ID = "%s:normal:1".formatted(CATALOG_NAME);
    private static final DataSize CROSS_REGION_READ_LIMIT = DataSize.of(17, Unit.MEGABYTE);
    private static final DataSize CROSS_REGION_WRITE_LIMIT = DataSize.of(17, Unit.MEGABYTE);

    private String accessKey;
    private String secretKey;
    private String region;
    private String bucket;
    private S3FileSystemFactory fileSystemFactory;
    private TrinoFileSystem fileSystem;

    @BeforeAll
    public void init()
    {
        Logging.initialize();

        initEnvironment();
        fileSystemFactory = createS3FileSystemFactory();
        fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
    }

    private void initEnvironment()
    {
        accessKey = environmentVariable("AWS_ACCESS_KEY_ID");
        secretKey = environmentVariable("AWS_SECRET_ACCESS_KEY");
        region = environmentVariable("AWS_REGION");
        bucket = environmentVariable("S3_BUCKET");
    }

    @AfterAll
    public void cleanup()
    {
        fileSystem = null;
        fileSystemFactory.destroy();
        fileSystemFactory = null;
    }

    @Test
    public void testS3FileSystemNetworkTracking()
            throws IOException
    {
        Location tempBlobLocation = createLocation("networkTracking/%s".formatted(UUID.randomUUID()));
        try {
            // write a 16 MB file
            TrinoOutputFile outputFile = fileSystem.newOutputFile(tempBlobLocation);
            try (OutputStream outputStream = outputFile.create()) {
                byte[] bytes = new byte[4];
                Slice slice = Slices.wrappedBuffer(bytes);
                for (int i = 0; i < 4 * BYTES_IN_MEGABYTE; i++) {
                    slice.setInt(0, i);
                    outputStream.write(bytes);
                }
            }

            // check that network monitor accurate tracked the bytes written with minimal difference
            CatalogNetworkMonitor catalogNetworkMonitor = CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor(CATALOG_NAME, CATALOG_ID, CROSS_REGION_READ_LIMIT.toBytes(), CROSS_REGION_WRITE_LIMIT.toBytes());
            assertThat(catalogNetworkMonitor.getCrossRegionWriteBytes()).isCloseTo(16 * BYTES_IN_MEGABYTE, Percentage.withPercentage(0.1));

            // read the 16 MB file in
            TrinoInputFile inputFile = fileSystem.newInputFile(tempBlobLocation);
            try (TrinoInputStream inputStream = inputFile.newStream()) {
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

            // check that network monitor accurate tracked the bytes read with minimal difference
            assertThat(catalogNetworkMonitor.getCrossRegionReadBytes()).isCloseTo(16 * BYTES_IN_MEGABYTE, Percentage.withPercentage(0.1));

            // check that cross-region limit works
            OutputStream outputStream = outputFile.create();
            outputStream.write(new byte[BYTES_IN_MEGABYTE]);
            assertThatThrownBy(outputStream::close).isInstanceOf(TrinoException.class).hasMessage("Cross-region write data transfer limit of 0GB per worker exceeded. To increase this limit, contact Starburst support.");
        }
        finally {
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

    private S3FileSystemFactory createS3FileSystemFactory()
    {
        return new S3FileSystemFactory(new S3FileSystemConfig()
                .setAwsAccessKey(accessKey)
                .setAwsSecretKey(secretKey)
                .setRegion(region)
                .setStreamingPartSize(DataSize.valueOf("5.5MB")),
                CatalogHandle.fromId(CATALOG_ID),
                new LocalRegionConfig()
                        // forces network traffic to be cross-region
                        .setAllowedIpAddresses(ImmutableList.of("0.0.0.0")),
                new CrossRegionConfig()
                        .setAllowCrossRegionAccess(true)
                        .setCrossRegionReadLimit(CROSS_REGION_READ_LIMIT)
                        .setCrossRegionWriteLimit(CROSS_REGION_WRITE_LIMIT));
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
