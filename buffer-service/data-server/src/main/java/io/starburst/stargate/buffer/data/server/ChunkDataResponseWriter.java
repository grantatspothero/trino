/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.starburst.stargate.buffer.data.execution.Chunk;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import static io.starburst.stargate.buffer.data.client.HttpDataClient.SERIALIZED_CHUNK_DATA_MAGIC;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA;

@Provider
@Produces(TRINO_CHUNK_DATA)
public class ChunkDataResponseWriter
        implements MessageBodyWriter<Chunk.ChunkDataRepresentation>
{
    private static final MediaType TRINO_CHUNK_DATA_TYPE = MediaType.valueOf(TRINO_CHUNK_DATA);

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return type == Chunk.ChunkDataRepresentation.class && mediaType.isCompatible(TRINO_CHUNK_DATA_TYPE);
    }

    @Override
    public long getSize(Chunk.ChunkDataRepresentation chunkDataRepresentation, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return Integer.BYTES // SERIALIZED_CHUNK_DATA_MAGIC
                + Long.BYTES // checksum
                + Integer.BYTES // num of data pages
                + chunkDataRepresentation.chunkSlices().stream().mapToInt(Slice::length).sum();
    }

    @Override
    public void writeTo(
            Chunk.ChunkDataRepresentation chunkDataRepresentation,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream output)
            throws IOException, WebApplicationException
    {
        try {
            SliceOutput sliceOutput = new OutputStreamSliceOutput(output);
            sliceOutput.writeInt(SERIALIZED_CHUNK_DATA_MAGIC);
            sliceOutput.writeLong(chunkDataRepresentation.checksum());
            sliceOutput.writeInt(chunkDataRepresentation.numDataPages());
            for (Slice slice : chunkDataRepresentation.chunkSlices()) {
                sliceOutput.writeBytes(slice);
            }
            // We use flush instead of close, because the underlying stream would be closed and that is not allowed.
            sliceOutput.flush();
        }
        catch (UncheckedIOException e) {
            // EOF exception occurs when the client disconnects while writing data
            // This is not a "server" problem so we don't want to log this
            if (!(e.getCause() instanceof EOFException)) {
                throw e;
            }
        }
    }
}
