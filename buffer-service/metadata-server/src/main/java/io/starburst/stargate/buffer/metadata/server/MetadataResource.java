/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.metadata.server;

import io.starburst.stargate.buffer.metadata.model.ChunksDao;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Produces(APPLICATION_JSON)
@Path("/api/v1/metadata")
public class MetadataResource
{
    private final ChunksDao chunksDao;

    @Inject
    public MetadataResource(Jdbi jdbi)
    {
        requireNonNull(jdbi, "jdbi is null");
        this.chunksDao = jdbi.onDemand(ChunksDao.class);
    }

    @GET
    @Path("chunk/{chunkId}")
    public Response getChunk(@PathParam("chunkId") long chunkId)
    {
        checkArgument(chunkId > 0, "negative chunk Id"); // artificial use of Guava to make deps checker happy
        return chunksDao.getChunk(chunkId)
                .map(Response::ok)
                .orElseGet(() -> Response.status(NOT_FOUND))
                .build();
    }
}
