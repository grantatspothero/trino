/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.azure;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import io.starburst.stargate.buffer.data.execution.ChunkManagerConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.AbstractSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpooledChunkNotFoundException;
import io.starburst.stargate.buffer.data.spooling.SpoolingUtils;
import reactor.core.publisher.Flux;

import javax.inject.Inject;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.getBucketName;
import static java.util.Objects.requireNonNull;

public class AzureBlobSpoolingStorage
        extends AbstractSpoolingStorage
{
    private final String containerName;
    private final BlobContainerAsyncClient containerClient;
    private final BlobBatchAsyncClient batchClient;
    private final long uploadBlockSize;
    private final int uploadMaxConcurrency;

    @Inject
    public AzureBlobSpoolingStorage(
            BufferNodeId bufferNodeId,
            ChunkManagerConfig chunkManagerConfig,
            DataServerStats dataServerStats,
            BlobServiceAsyncClient blobServiceAsyncClient,
            AzureBlobSpoolingConfig azureBlobSpoolingConfig)
    {
        super(bufferNodeId, dataServerStats);

        this.containerName = getBucketName(requireNonNull(chunkManagerConfig.getSpoolingDirectory(), "spoolingDirectory is null"));
        this.containerClient = requireNonNull(blobServiceAsyncClient, "blobServiceAsyncClient is null").getBlobContainerAsyncClient(containerName);
        this.batchClient = new BlobBatchClientBuilder(containerClient).buildAsyncClient();
        requireNonNull(azureBlobSpoolingConfig, "azureBlobSpoolingConfig is null");
        this.uploadBlockSize = azureBlobSpoolingConfig.getUploadBlockSize().toBytes();
        this.uploadMaxConcurrency = azureBlobSpoolingConfig.getUploadMaxConcurrency();
    }

    @Override
    protected int getFileSize(String fileName) throws SpooledChunkNotFoundException
    {
        try {
            return (int) containerClient.getBlobAsyncClient(fileName).getProperties().block().getBlobSize();
        }
        catch (BlobStorageException e) {
            throw new SpooledChunkNotFoundException(e);
        }
    }

    @Override
    protected String getLocation(String fileName)
    {
        // this isn't actually a valid Azure URI (which has the account name as the first part),
        // but only the reader uses this to sanity check the scheme and get the container and file name.
        return "ms://" + containerName + PATH_SEPARATOR + fileName;
    }

    @Override
    protected ListenableFuture<Void> deleteDirectories(List<String> directoryNames)
    {
        CompletableFuture<Void> result = Flux.fromIterable(directoryNames)
                .flatMap(directoryName -> containerClient.listBlobs(new ListBlobsOptions().setPrefix(directoryName)))
                .map(blobItem -> containerClient.getBlobContainerUrl() + PATH_SEPARATOR + blobItem.getName())
                .buffer(256) // Azure's max batch size
                .flatMap(batch -> batchClient.deleteBlobs(batch, DeleteSnapshotsOptionType.INCLUDE))
                .then()
                .toFuture();

        return toListenableFuture(result);
    }

    @Override
    protected ListenableFuture<?> putStorageObject(String fileName, ChunkDataLease chunkDataLease)
    {
        BlobAsyncClient blobClient = containerClient.getBlobAsyncClient(fileName);
        Flux<ByteBuffer> parts = Flux.create(fluxSink -> {
            SpoolingUtils.writeChunkDataLease(chunkDataLease, fluxSink::next);
            fluxSink.complete();
        });
        CompletableFuture<BlockBlobItem> future = blobClient.upload(parts, new ParallelTransferOptions()
                .setBlockSizeLong(uploadBlockSize)
                .setMaxConcurrency(uploadMaxConcurrency), true)
                .toFuture();
        return toListenableFuture(future);
    }

    @Override
    public void close() throws Exception {}
}