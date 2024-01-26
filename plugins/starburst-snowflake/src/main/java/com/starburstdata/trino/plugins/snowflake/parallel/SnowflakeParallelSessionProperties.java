/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;

public class SnowflakeParallelSessionProperties
        implements SessionPropertiesProvider
{
    public static final String CLIENT_RESULT_CHUNK_SIZE = "client_result_chunk_size";

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return ImmutableList.of(
                dataSizeProperty(
                        CLIENT_RESULT_CHUNK_SIZE,
                        "Max result chink size (MB)",
                        DataSize.of(160, DataSize.Unit.MEGABYTE),
                        true));
    }

    public static Optional<DataSize> getResultChunkSize(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(CLIENT_RESULT_CHUNK_SIZE, DataSize.class));
    }
}
