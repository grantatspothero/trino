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
package io.trino.filesystem.gcs.galaxy;

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGcsLocationRegionValidation
{
    private static final String SINGLE_REGION_NAME = "us-east1";
    private static final String DUAL_REGION_NAME = "NAM4";
    private static GcsStorageFactory storageFactory;
    private static Storage storage;
    private static String singleRegionBucketName;
    private static String dualRegionBucketName;

    private final ConnectorIdentity identity = ConnectorIdentity.ofUser("test");

    @BeforeAll
    static void init()
            throws Exception
    {
        String gcpCredentialKey = environmentVariable("GCP_CREDENTIALS_KEY");

        // Note: the account needs the following permissions:
        // create/get/delete bucket
        // create/get/list/delete blob
        // For gcp testing this corresponds to the Cluster Storage Admin and Cluster Storage Object Admin roles
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        GcsFileSystemConfig config = new GcsFileSystemConfig().setJsonKey(new String(jsonKeyBytes, UTF_8));
        storageFactory = new GcsStorageFactory(config);
        storage = storageFactory.create(ConnectorIdentity.ofUser("test"));
        singleRegionBucketName = RemoteStorageHelper.generateBucketName();
        storage.create(BucketInfo.newBuilder(singleRegionBucketName)
                .setLocation(SINGLE_REGION_NAME)
                .build());
        dualRegionBucketName = RemoteStorageHelper.generateBucketName();
        storage.create(BucketInfo.newBuilder(dualRegionBucketName)
                .setLocation(DUAL_REGION_NAME)
                .build());
    }

    @AfterAll
    static void tearDown()
    {
        try {
            storage.delete(singleRegionBucketName);
            storage.delete(dualRegionBucketName);
        }
        finally {
            storage = null;
        }
    }

    @Test
    public void testDisabledValidation()
    {
        GcsRegionEnforcementConfig config = new GcsRegionEnforcementConfig();
        assertThat(createFileSystemFactory(config).validate(identity, createLocation(singleRegionBucketName, "some/location"))).isEmpty();
    }

    @Test
    public void testAgainstEnforcedRegion()
    {
        GcsRegionEnforcementConfig config = new GcsRegionEnforcementConfig()
                .setEnforcedRegion(SINGLE_REGION_NAME);
        TrinoFileSystemFactory fileSystemFactory = createFileSystemFactory(config);
        assertThat(fileSystemFactory.validate(identity, createLocation(singleRegionBucketName, "some/location"))).isEmpty();
        assertThat(fileSystemFactory.validate(identity, createLocation(singleRegionBucketName, "some/other/location"))).isEmpty();
    }

    @Test
    public void testAgainstDisallowedRegion()
    {
        GcsRegionEnforcementConfig config = new GcsRegionEnforcementConfig()
                .setEnforcedRegion("me-south-1");
        assertThat(createFileSystemFactory(config).validate(identity, createLocation(singleRegionBucketName, "some/location")))
                .isEqualTo(Optional.of("Google Cloud Storage bucket %s is in regions [%s], but only region me-south-1 is allowed".formatted(singleRegionBucketName, SINGLE_REGION_NAME)));
    }

    @Test
    public void testDualRegionWithAllowedRegion()
    {
        GcsRegionEnforcementConfig config = new GcsRegionEnforcementConfig()
                .setEnforcedRegion(SINGLE_REGION_NAME);
        TrinoFileSystemFactory fileSystemFactory = createFileSystemFactory(config);
        assertThat(fileSystemFactory.validate(identity, createLocation(dualRegionBucketName, "some/location"))).isEmpty();
        assertThat(fileSystemFactory.validate(identity, createLocation(dualRegionBucketName, "some/other/location"))).isEmpty();
    }

    @Test
    public void testInvalidDualRegionBucket()
    {
        GcsRegionEnforcementConfig config = new GcsRegionEnforcementConfig()
                .setEnforcedRegion("me-south-1");
        TrinoFileSystemFactory fileSystemFactory = createFileSystemFactory(config);
        assertThat(fileSystemFactory.validate(identity, createLocation(dualRegionBucketName, "some/location")))
                .isEqualTo(Optional.of("Google Cloud Storage bucket %s is in regions [us-central1, us-east1], but only region me-south-1 is allowed".formatted(dualRegionBucketName)));
    }

    private static String environmentVariable(String name)
    {
        return requireNonNull(System.getenv(name), "Environment variable not set: " + name);
    }

    private TrinoFileSystemFactory createFileSystemFactory(GcsRegionEnforcementConfig regionEnforcementConfig)
    {
        return new GcsFileSystemFactory(new GcsFileSystemConfig(), regionEnforcementConfig, storageFactory);
    }

    private static Location createLocation(String bucket, String path)
    {
        return Location.of("gs://%s/%s".formatted(bucket, path));
    }
}
