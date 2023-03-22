/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.gcs;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.google.common.base.Verify.verify;

public class StorageProvider
        implements Provider<Storage>
{
    private final GcsClientConfig gcsClientConfig;

    @Inject
    public StorageProvider(GcsClientConfig gcsClientConfig)
    {
        this.gcsClientConfig = gcsClientConfig;
    }

    @Override
    public Storage get()
    {
        try {
            StorageOptions.Builder options = StorageOptions.newBuilder()
                    .setStorageRetryStrategy(gcsClientConfig.getStorageRetryStrategy());
            gcsClientConfig.getGcsEndpoint().ifPresent(options::setHost);

            Optional<String> gcsJsonKeyFilePath = gcsClientConfig.getGcsJsonKeyFilePath();
            Optional<String> gcsJsonKey = gcsClientConfig.getGcsJsonKey();

            if (gcsJsonKeyFilePath.isPresent()) {
                verify(gcsJsonKey.isEmpty(), "gcsJsonKeyFilePath should not be set with gcsJsonKey");
                Credentials credentials = GoogleCredentials.fromStream(new FileInputStream(gcsJsonKeyFilePath.get()));
                options.setCredentials(credentials);
            }
            else if (gcsJsonKey.isPresent()) {
                Credentials credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(gcsJsonKey.get().getBytes(StandardCharsets.UTF_8)));
                options.setCredentials(credentials);
            }

            return options.build().getService();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create GCS Storage client", e);
        }
    }
}
