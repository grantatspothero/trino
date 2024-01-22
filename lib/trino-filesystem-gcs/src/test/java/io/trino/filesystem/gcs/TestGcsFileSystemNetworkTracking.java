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
package io.trino.filesystem.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.gcs.galaxy.GalaxyTransportOptionsConfigurer;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.galaxy.CatalogNetworkMonitor;
import io.trino.spi.security.ConnectorIdentity;
import org.assertj.core.data.Offset;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.filesystem.gcs.GcsUtils.getBlobOrThrow;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
public class TestGcsFileSystemNetworkTracking
{
    private static final int BYTES_IN_MEGABYTE = 1024 * 1024;
    private static final String GCP_CREDENTIAL_KEY = requireNonNull(System.getenv("GCP_CREDENTIALS_KEY"), "GCP_CREDENTIALS_KEY is not set");
    private static final DataSize HIGH_CROSS_REGION_LIMIT = DataSize.of(1, Unit.GIGABYTE);
    private static final DataSize ZERO_CROSS_REGION_LIMIT = DataSize.ofBytes(0);
    private static final ConnectorIdentity TESTING_CONNECTOR_IDENTITY = ConnectorIdentity.ofUser("test");

    private Location rootLocation;

    @BeforeAll
    void setup()
            throws Exception
    {
        try (Storage storage = createStorageClient()) {
            String bucket = RemoteStorageHelper.generateBucketName();
            storage.create(BucketInfo.of(bucket));
            this.rootLocation = Location.of("gs://%s/".formatted(bucket));
        }
    }

    @AfterAll
    void cleanup()
            throws Exception
    {
        try (Storage storage = createStorageClient()) {
            RemoteStorageHelper.forceDelete(storage, rootLocation.host().get(), 5, TimeUnit.SECONDS);
        }
    }

    @Test
    @Order(1)
    public void testGcsFileSystemEnforcesCrossRegionWriteLimits()
            throws IOException
    {
        String catalogName = newCatalogName();
        String catalogId = createCatalogId(catalogName);
        GcsFileSystemFactory fileSystemFactory = createGcsFileSystemFactory(catalogId, HIGH_CROSS_REGION_LIMIT, ZERO_CROSS_REGION_LIMIT);
        TrinoFileSystem fileSystem = fileSystemFactory.create(TESTING_CONNECTOR_IDENTITY);
        Location tempBlobLocation = createLocation("writeLimitEnforcing/%s".formatted(UUID.randomUUID()));
        CatalogNetworkMonitor catalogNetworkMonitor = CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, HIGH_CROSS_REGION_LIMIT.toBytes(), ZERO_CROSS_REGION_LIMIT.toBytes());

        try {
            TrinoOutputFile outputFile = fileSystem.newOutputFile(tempBlobLocation);
            assertThatThrownBy(outputFile::create).hasRootCauseInstanceOf(TrinoException.class).hasRootCauseMessage("Cross-region write data transfer limit of 0GB per worker exceeded. To increase this limit, contact Starburst support.");

            // It is expected that a small amount of bytes was transferred
            // TODO: Check limits before writing https://github.com/starburstdata/galaxy-trino/issues/1259
            assertThat(catalogNetworkMonitor.getCrossRegionWriteBytes()).isCloseTo(0, Offset.offset(2000L));
        }
        finally {
            fileSystemFactory.stop();
            try (Storage storage = createStorageClient()) {
                Blob blob = getBlobOrThrow(storage, new GcsLocation(tempBlobLocation));
                blob.delete();
            }
            catch (Exception ignored) {
            }
        }
    }

    @Test
    @Order(2)
    public void testGcsFileSystemEnforcesCrossRegionReadLimits()
            throws IOException
    {
        String catalogName = newCatalogName();
        String catalogId = createCatalogId(catalogName);
        GcsFileSystemFactory fileSystemFactory = createGcsFileSystemFactory(catalogId, ZERO_CROSS_REGION_LIMIT, HIGH_CROSS_REGION_LIMIT);
        TrinoFileSystem fileSystem = fileSystemFactory.create(TESTING_CONNECTOR_IDENTITY);
        Location tempBlobLocation = createLocation("readLimitEnforcing/%s".formatted(UUID.randomUUID()));
        CatalogNetworkMonitor catalogNetworkMonitor = CatalogNetworkMonitor.getCrossRegionCatalogNetworkMonitor(catalogName, catalogId, ZERO_CROSS_REGION_LIMIT.toBytes(), HIGH_CROSS_REGION_LIMIT.toBytes());

        try {
            TrinoInputFile inputFile = fileSystem.newInputFile(tempBlobLocation);
            assertThatThrownBy(inputFile::newStream).hasRootCauseInstanceOf(TrinoException.class).hasRootCauseMessage("Cross-region read data transfer limit of 0GB per worker exceeded. To increase this limit, contact Starburst support.");

            // It is expected that a small amount of bytes was transferred
            assertThat(catalogNetworkMonitor.getCrossRegionReadBytes()).isCloseTo(0, Offset.offset(1000L));
        }
        finally {
            fileSystemFactory.stop();
            try (Storage storage = createStorageClient()) {
                Blob blob = getBlobOrThrow(storage, new GcsLocation(tempBlobLocation));
                blob.delete();
            }
            catch (Exception ignored) {
            }
        }
    }

    @Test
    @Order(3)
    public void testGcsFileSystemAccurateNetworkTracking()
            throws IOException
    {
        String catalogName = newCatalogName();
        String catalogId = createCatalogId(catalogName);
        GcsFileSystemFactory fileSystemFactory = createGcsFileSystemFactory(catalogId, HIGH_CROSS_REGION_LIMIT, HIGH_CROSS_REGION_LIMIT);
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
            fileSystemFactory.stop();
            try (Storage storage = createStorageClient()) {
                Blob blob = getBlobOrThrow(storage, new GcsLocation(tempBlobLocation));
                blob.delete();
            }
            catch (Exception ignored) {
            }
        }
    }

    private GcsFileSystemFactory createGcsFileSystemFactory(String catalogId, DataSize crossRegionReadLimit, DataSize crossRegionWriteLimit)
            throws IOException
    {
        GcsFileSystemConfig config = createGcsFileSystemConfig();
        CatalogHandle catalogHandle = CatalogHandle.fromId(catalogId);
        LocalRegionConfig localRegionConfig = new LocalRegionConfig()
                .setAllowedIpAddresses(ImmutableList.of("0.0.0.0")); // forces network traffic to be cross-region
        CrossRegionConfig crossRegionConfig = new CrossRegionConfig()
                .setAllowCrossRegionAccess(true)
                .setCrossRegionReadLimit(crossRegionReadLimit)
                .setCrossRegionWriteLimit(crossRegionWriteLimit);
        GalaxyTransportOptionsConfigurer configurer = new GalaxyTransportOptionsConfigurer(catalogHandle, localRegionConfig, crossRegionConfig);
        return new GcsFileSystemFactory(config, new GcsStorageFactory(ImmutableSet.of(configurer), config));
    }

    private static Storage createStorageClient()
            throws IOException
    {
        return new GcsStorageFactory(createGcsFileSystemConfig()).create(TESTING_CONNECTOR_IDENTITY);
    }

    private static GcsFileSystemConfig createGcsFileSystemConfig()
    {
        return new GcsFileSystemConfig()
                .setJsonKey(new String(Base64.getDecoder().decode(GCP_CREDENTIAL_KEY), UTF_8));
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
        return rootLocation.appendPath(path);
    }
}
