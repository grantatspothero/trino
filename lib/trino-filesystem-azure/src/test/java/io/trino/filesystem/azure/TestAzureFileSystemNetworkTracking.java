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
package io.trino.filesystem.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.StorageAccountInfo;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.options.DataLakePathDeleteOptions;
import com.google.common.collect.ImmutableList;
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
import io.trino.filesystem.azure.AbstractTestAzureFileSystem.AccountKind;
import io.trino.plugin.base.galaxy.CrossRegionConfig;
import io.trino.plugin.base.galaxy.LocalRegionConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.galaxy.CatalogNetworkMonitor;
import io.trino.spi.security.ConnectorIdentity;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import static com.azure.storage.blob.models.AccountKind.BLOB_STORAGE;
import static com.azure.storage.blob.models.AccountKind.STORAGE_V2;
import static com.azure.storage.common.Utility.urlEncode;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.filesystem.azure.AbstractTestAzureFileSystem.AccountKind.BLOB;
import static io.trino.filesystem.azure.AbstractTestAzureFileSystem.AccountKind.FLAT;
import static io.trino.filesystem.azure.AbstractTestAzureFileSystem.AccountKind.HIERARCHICAL;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(Lifecycle.PER_CLASS)
public class TestAzureFileSystemNetworkTracking
{
    private static final int BYTES_IN_MEGABYTE = 1024 * 1024;
    private static final String CATALOG_NAME = "catalog";
    private static final String CATALOG_ID = "%s:normal:1".formatted(CATALOG_NAME);
    private static final DataSize CROSS_REGION_READ_LIMIT = DataSize.of(17, Unit.MEGABYTE);
    private static final DataSize CROSS_REGION_WRITE_LIMIT = DataSize.of(17, Unit.MEGABYTE);

    private String account;
    private StorageSharedKeyCredential credential;
    private AccountKind accountKind;
    private String containerName;
    private Location rootLocation;
    private BlobContainerClient blobContainerClient;
    private TrinoFileSystem fileSystem;

    @BeforeAll
    void setup()
            throws IOException
    {
        this.account = getRequiredEnvironmentVariable("ABFS_ACCOUNT");
        String accountKey = getRequiredEnvironmentVariable("ABFS_ACCESS_KEY");
        AccountKind expectedAccountKind = FLAT;
        credential = new StorageSharedKeyCredential(account, accountKey);

        String blobEndpoint = "https://%s.blob.core.windows.net".formatted(account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(blobEndpoint)
                .credential(credential)
                .buildClient();
        accountKind = getAccountKind(blobServiceClient);
        checkState(accountKind == expectedAccountKind, "Expected %s account, but found %s".formatted(expectedAccountKind, accountKind));

        containerName = "test-%s-%s".formatted(accountKind.name().toLowerCase(ROOT), randomUUID());
        rootLocation = Location.of("abfs://%s@%s.dfs.core.windows.net/".formatted(containerName, account));

        blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
        // this will fail if the container already exists, which is what we want
        blobContainerClient.create();

        fileSystem = new AzureFileSystemFactory(
                OpenTelemetry.noop(),
                new AzureAuthAccessKey(accountKey),
                new AzureFileSystemConfig(),
                CatalogHandle.fromId(CATALOG_ID),
                new LocalRegionConfig()
                        .setAllowedIpAddresses(ImmutableList.of("0.0.0.0")),
                new CrossRegionConfig()
                        .setAllowCrossRegionAccess(true)
                        .setCrossRegionReadLimit(CROSS_REGION_READ_LIMIT)
                        .setCrossRegionWriteLimit(CROSS_REGION_WRITE_LIMIT))
                .create(ConnectorIdentity.ofUser("test"));

        cleanupFiles();
    }

    @AfterAll
    void tearDown()
    {
        credential = null;
        fileSystem = null;
        if (blobContainerClient != null) {
            blobContainerClient.deleteIfExists();
            blobContainerClient = null;
        }
    }

    @AfterEach
    void afterEach()
    {
        cleanupFiles();
    }

    private static AccountKind getAccountKind(BlobServiceClient blobServiceClient)
            throws IOException
    {
        StorageAccountInfo accountInfo = blobServiceClient.getAccountInfo();
        if (accountInfo.getAccountKind() == STORAGE_V2) {
            if (accountInfo.isHierarchicalNamespaceEnabled()) {
                return HIERARCHICAL;
            }
            return FLAT;
        }
        if (accountInfo.getAccountKind() == BLOB_STORAGE) {
            return BLOB;
        }
        throw new IOException("Unsupported account kind '%s'".formatted(accountInfo.getAccountKind()));
    }

    private void cleanupFiles()
    {
        if (accountKind == HIERARCHICAL) {
            DataLakeFileSystemClient fileSystemClient = new DataLakeFileSystemClientBuilder()
                    .endpoint("https://%s.dfs.core.windows.net".formatted(account))
                    .fileSystemName(containerName)
                    .credential(credential)
                    .buildClient();

            DataLakePathDeleteOptions deleteRecursiveOptions = new DataLakePathDeleteOptions().setIsRecursive(true);
            for (PathItem pathItem : fileSystemClient.listPaths()) {
                if (pathItem.isDirectory()) {
                    fileSystemClient.deleteDirectoryIfExistsWithResponse(pathItem.getName(), deleteRecursiveOptions, null, null);
                }
                else {
                    fileSystemClient.deleteFileIfExists(pathItem.getName());
                }
            }
        }
        else {
            blobContainerClient.listBlobs().forEach(item -> blobContainerClient.getBlobClient(urlEncode(item.getName())).deleteIfExists());
        }
    }

    @Test
    public void testAzureFileSystemNetworkTracking()
            throws IOException
    {
        Location tempBlobLocation = createLocation("networkTracking/%s".formatted(UUID.randomUUID()));
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

        // check that network monitor accurately tracked the bytes written with minimal difference
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

        // check that network monitor accurately tracked the bytes read with minimal difference
        assertThat(catalogNetworkMonitor.getCrossRegionReadBytes()).isCloseTo(16 * BYTES_IN_MEGABYTE, Percentage.withPercentage(0.1));

        fileSystem.deleteFile(tempBlobLocation);
        // check that cross-region limit works
        OutputStream outputStream = outputFile.create();
        outputStream.write(new byte[BYTES_IN_MEGABYTE]);
        assertThatThrownBy(outputStream::close).hasRootCause(new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Cross-region write data transfer limit of 0GB per worker exceeded. To increase this limit, contact Starburst support."));
    }

    private Location createLocation(String path)
    {
        return rootLocation.appendPath(path);
    }

    private static String getRequiredEnvironmentVariable(String name)
    {
        return requireNonNull(System.getenv(name), "Environment variable not set: " + name);
    }
}
