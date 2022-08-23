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

import java.util.Queue;
import java.util.function.ToLongFunction;

import static io.airlift.slice.SizeOf.sizeOfObjectArray;

public final class MoreSizeOf
{
    public static final int OBJECT_HEADER_SIZE = 16; /* object header with possible padding */

    private MoreSizeOf() {}

    // todo: replace with SizeOf::estimatedSizeOf when https://github.com/airlift/slice/pull/151 lands and gets released
    public static <T> long estimatedSizeOf(Queue<T> queue, ToLongFunction<T> valueSize)
    {
        if (queue == null) {
            return 0;
        }

        long result = sizeOfObjectArray(queue.size());
        for (T value : queue) {
            result += valueSize.applyAsLong(value);
        }
        return result;
    }
}
