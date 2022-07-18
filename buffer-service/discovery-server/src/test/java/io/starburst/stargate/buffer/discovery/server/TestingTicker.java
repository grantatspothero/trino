/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.server;

import com.google.common.base.Ticker;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

// TODO (https://github.com/airlift/airlift/pull/998) replace usages with io.airlift.testing.TestingTicker when PR merges
@ThreadSafe
public class TestingTicker
        extends Ticker
{
    private volatile long time;

    @Override
    public long read()
    {
        return time;
    }

    public synchronized void increment(long delta, TimeUnit unit)
    {
        checkArgument(delta >= 0, "delta is negative");
        time += unit.toNanos(delta);
    }
}
