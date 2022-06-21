/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package io.starburst.stargate.buffer.metadata.model;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ChunkRowMapper
        implements RowMapper<Chunk>
{
    @Override
    public Chunk map(ResultSet rs, StatementContext ctx)
            throws SQLException
    {
        return new Chunk(rs.getLong("chunk_id"));
    }
}
