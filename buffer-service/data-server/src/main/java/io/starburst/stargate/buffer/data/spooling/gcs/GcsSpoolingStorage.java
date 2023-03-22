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

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.AbstractSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpooledChunkNotFoundException;
import io.starburst.stargate.buffer.data.spooling.SpoolingUtils;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;
import static io.starburst.stargate.buffer.data.client.spooling.gcs.GcsSpoolUtils.getBucketName;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GcsSpoolingStorage
            extends AbstractSpoolingStorage
{
    private static final int MAX_BATCH_REQUEST_SIZE = 100;

    private final String bucketName;
    private final Storage gcsClient;
    private final ExecutorService executor;

    @Inject
    public GcsSpoolingStorage(
            BufferNodeId bufferNodeId,
            ChunkManagerConfig chunkManagerConfig,
            GcsClientConfig gcsClientConfig,
            Storage gcsClient,
            DataServerStats dataServerStats)
    {
        super(bufferNodeId, dataServerStats);

        this.bucketName = getBucketName(requireNonNull(chunkManagerConfig.getSpoolingDirectory(), "spoolingDirectory is null"));
        this.gcsClient = gcsClient;

        this.executor = new ThreadPoolExecutor(
                gcsClientConfig.getThreadCount(),
                gcsClientConfig.getThreadCount(),
                60L,
                SECONDS,
                new LinkedBlockingQueue<>(),
                threadsNamed("gcs-spooling-%s"));
    }

    @Override
    protected int getFileSize(String fileName) throws SpooledChunkNotFoundException
    {
        Blob blob = gcsClient.get(BlobId.of(bucketName, fileName));
        if (blob == null) {
            throw new SpooledChunkNotFoundException();
        }
        return blob.getSize().intValue();
    }

    @Override
    protected String getLocation(String fileName)
    {
        return "gs://" + bucketName + PATH_SEPARATOR + fileName;
    }

    @Override
    protected ListenableFuture<Void> deleteDirectories(List<String> directoryNames)
    {
        ImmutableList.Builder<ListenableFuture<List<String>>> listObjectsFuturesBuilder = ImmutableList.builder();
        for (String directoryName : directoryNames) {
            ListenableFuture<List<String>> directoryFuture = Futures.submit(() -> {
                Page<Blob> page = gcsClient.list(bucketName, Storage.BlobListOption.prefix(directoryName));
                ImmutableList.Builder<String> keys = ImmutableList.builder();
                for (Blob blob : page.iterateAll()) {
                    keys.add(blob.getName());
                }
                return keys.build();
            }, executor);
            listObjectsFuturesBuilder.add(directoryFuture);
        }
        return asVoid(Futures.transformAsync(
                Futures.allAsList(listObjectsFuturesBuilder.build()),
                nestedList -> {
                    List<String> flattenedList = nestedList.stream()
                            .flatMap(Collection::stream)
                            .collect(toImmutableList());
                    List<ListenableFuture<Void>> batches = Lists.partition(flattenedList, MAX_BATCH_REQUEST_SIZE).stream()
                            .map(fileBatch -> {
                                StorageBatch batchRequest = gcsClient.batch();
                                for (String file : fileBatch) {
                                    batchRequest.delete(bucketName, file);
                                }
                                return Futures.submit(batchRequest::submit, executor);
                            }).collect(toImmutableList());
                    return Futures.allAsList(batches);
                }, executor));
    }

    @Override
    protected ListenableFuture<?> putStorageObject(String fileName, ChunkDataLease chunkDataLease)
    {
        return Futures.submit(() -> {
            BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, fileName).build();
            try (WriteChannel writer = gcsClient.writer(blobInfo)) {
                SpoolingUtils.writeChunkDataLease(chunkDataLease, buffer -> {
                    try {
                        writer.write(buffer);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return blobInfo;
        }, executor);
    }

    @Override
    public void close() throws Exception
    {
        try (Closer closer = Closer.create()) {
            closer.register(executor::shutdown);
        }
    }
}
