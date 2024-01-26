/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.distributed;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveSplit;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// Snowflake split contains some fields from io.trino.plugin.hive.HiveSplit
public class SnowflakeSplit
        implements ConnectorSplit
{
    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final Map<String, String> schema;
    private final List<HostAddress> addresses;
    private final boolean forceLocalScheduling;

    private final SnowflakeStageAccessInfo stageAccessInfo;

    SnowflakeSplit(
            HiveSplit hiveSplit,
            SnowflakeStageAccessInfo stageAccessInfo)
    {
        this(
                hiveSplit.getPath(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getEstimatedFileSize(),
                hiveSplit.getSchema(),
                hiveSplit.getAddresses(),
                hiveSplit.isForceLocalScheduling(),
                stageAccessInfo);
    }

    @JsonCreator
    public SnowflakeSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("schema") Map<String, String> schema,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("forceLocalScheduling") boolean forceLocalScheduling,
            @JsonProperty("stageAccessInfo") SnowflakeStageAccessInfo stageAccessInfo)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(fileSize >= 0, "fileSize must be positive");
        requireNonNull(path, "path is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(addresses, "addresses is null");

        this.path = path;
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.schema = schema;
        this.addresses = ImmutableList.copyOf(addresses);
        this.forceLocalScheduling = forceLocalScheduling;

        this.stageAccessInfo = requireNonNull(stageAccessInfo, "stageAccessInfo is null");
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonProperty
    public Map<String, String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return !forceLocalScheduling;
    }

    @JsonProperty
    public SnowflakeStageAccessInfo getStageAccessInfo()
    {
        return stageAccessInfo;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path)
                .put("start", start)
                .put("length", length)
                .put("fileSize", fileSize)
                .put("hosts", addresses)
                .buildOrThrow();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .addValue(fileSize)
                .addValue(stageAccessInfo)
                .toString();
    }
}
