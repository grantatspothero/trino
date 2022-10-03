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

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;

import static io.starburst.stargate.buffer.data.client.TrinoMediaTypes.TRINO_PAGES;

@Provider
@Consumes(TRINO_PAGES)
public class PagesResponseReader
        implements MessageBodyReader<List<Slice>>
{
    private static final MediaType TRINO_PAGES_TYPE = MediaType.valueOf(TRINO_PAGES);
    private static final Type LIST_GENERIC_TOKEN;

    static {
        try {
            LIST_GENERIC_TOKEN = List.class.getMethod("get", int.class).getGenericReturnType();
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return List.class.isAssignableFrom(type) &&
                TypeToken.of(genericType).resolveType(LIST_GENERIC_TOKEN).getRawType().equals(Slice.class) &&
                mediaType.isCompatible(TRINO_PAGES_TYPE);
    }

    @Override
    public List<Slice> readFrom(
            Class<List<Slice>> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, String> multivaluedMap,
            InputStream inputStream)
            throws WebApplicationException
    {
        ImmutableList.Builder<Slice> pages = ImmutableList.builder();
        SliceInput sliceInput = new InputStreamSliceInput(inputStream);
        while (sliceInput.isReadable()) {
            pages.add(sliceInput.readSlice(sliceInput.readInt()));
        }
        return pages.build();
    }
}
