/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import com.google.inject.Inject;
import io.trino.spi.exchange.ExchangeId;

import static java.util.Objects.requireNonNull;

public class RandomPartitionNodeMapperFactory
        implements PartitionNodeMapperFactory
{
    private final BufferNodeDiscoveryManager discoveryManager;

    @Inject
    public RandomPartitionNodeMapperFactory(BufferNodeDiscoveryManager discoveryManager)
    {
        this.discoveryManager = requireNonNull(discoveryManager, "discoveryManager is null");
    }

    @Override
    public PartitionNodeMapper getPartitionNodeMapper(ExchangeId exchangeId, int outputPartitionCount, boolean preserveOrderWithinPartition)
    {
        return new RandomPartitionNodeMapper(discoveryManager, outputPartitionCount);
    }
}