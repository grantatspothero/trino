/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.SpoolingFile;
import io.starburst.stargate.buffer.data.spooling.gcs.GcsClientConfig;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.getBucketName;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.keyFromUri;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.toDataPages;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GcsSpooledChunkReader
        implements SpooledChunkReader
{
    private final Storage gcsClient;
    private final boolean dataIntegrityVerificationEnabled;
    private final ExecutorService executor;

    @Inject
    public GcsSpooledChunkReader(
            DataApiConfig dataApiConfig,
            GcsClientConfig gcsClientConfig,
            Storage gcsClient)
    {
        this.dataIntegrityVerificationEnabled = dataApiConfig.isDataIntegrityVerificationEnabled();
        this.gcsClient = requireNonNull(gcsClient, "gcsClient is null");
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                gcsClientConfig.getThreadCount(),
                gcsClientConfig.getThreadCount(),
                60L,
                SECONDS,
                new LinkedBlockingQueue<>(),
                threadsNamed("gcs-spooling-%s"));
        executor.allowCoreThreadTimeOut(true);
        this.executor = executor;
    }

    @Override
    public ListenableFuture<List<DataPage>> getDataPages(SpoolingFile spoolingFile)
    {
        URI spoolingFileUri = URI.create(spoolingFile.location());

        String scheme = spoolingFileUri.getScheme();
        checkArgument(spoolingFileUri.getScheme().equals("gs"), "Unexpected storage scheme %s for GcsSpooledChunkReader, expecting gcs", scheme);

        return Futures.submit(() -> {
            Blob blob = gcsClient.get(BlobId.of(getBucketName(spoolingFileUri), keyFromUri(spoolingFileUri)));
            return toDataPages(blob.getContent(), dataIntegrityVerificationEnabled);
        }, executor);
    }

    @Override
    public void close()
            throws Exception
    {
        try (Closer closer = Closer.create()) {
            closer.register(executor::shutdown);
        }
    }
}
