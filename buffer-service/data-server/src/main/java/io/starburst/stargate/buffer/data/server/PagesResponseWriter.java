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

import com.google.common.reflect.TypeToken;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import io.starburst.stargate.buffer.data.client.DataPage;

import javax.inject.Inject;
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
import java.util.List;

import static io.starburst.stargate.buffer.data.client.HttpDataClient.SERIALIZED_PAGES_MAGIC;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.calculateChecksum;
import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_CHUNK_DATA;
import static java.util.Objects.requireNonNull;

@Provider
@Produces(TRINO_CHUNK_DATA)
public class PagesResponseWriter
        implements MessageBodyWriter<List<DataPage>>
{
    private static final MediaType TRINO_CHUNK_DATA_TYPE = MediaType.valueOf(TRINO_CHUNK_DATA);
    private static final Type LIST_GENERIC_TOKEN;

    static {
        try {
            LIST_GENERIC_TOKEN = List.class.getMethod("get", int.class).getGenericReturnType();
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private final boolean includeChecksum;

    @Inject
    public PagesResponseWriter(DataServerConfig dataServerConfig)
    {
        requireNonNull(dataServerConfig, "dataServerConfig is null");
        this.includeChecksum = dataServerConfig.getIncludeChecksumInDataResponse();
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return List.class.isAssignableFrom(type) &&
                TypeToken.of(genericType).resolveType(LIST_GENERIC_TOKEN).getRawType().equals(DataPage.class) &&
                mediaType.isCompatible(TRINO_CHUNK_DATA_TYPE);
    }

    @Override
    public long getSize(List<DataPage> pages, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return Integer.BYTES // SERIALIZED_PAGES_MAGIC
                + Long.BYTES // checkSum
                + Integer.BYTES // list size
                + pages.stream().mapToInt(dataPage -> DATA_PAGE_HEADER_SIZE + dataPage.data().length()).sum();
    }

    @Override
    public void writeTo(
            List<DataPage> pages,
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
            sliceOutput.writeInt(SERIALIZED_PAGES_MAGIC);
            sliceOutput.writeLong(includeChecksum ? calculateChecksum(pages) : NO_CHECKSUM);
            sliceOutput.writeInt(pages.size());
            for (DataPage page : pages) {
                sliceOutput.writeShort(page.taskId()); // addDataPage() guarantees taskId is no more than 32767
                sliceOutput.writeByte(page.attemptId()); // addDataPage() guarantees attemptId is no more than 127
                sliceOutput.writeInt(page.data().length());
                sliceOutput.writeBytes(page.data());
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
