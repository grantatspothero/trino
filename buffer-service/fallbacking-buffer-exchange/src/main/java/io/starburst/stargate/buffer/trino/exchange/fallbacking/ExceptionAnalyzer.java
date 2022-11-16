/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange.fallbacking;

public class ExceptionAnalyzer
{
    public boolean isInternalException(Throwable t)
    {
        if (isInternalExceptionNoCauses(t)) {
            return true;
        }
        if (t.getCause() == null || t.getCause() == t) {
            return false;
        }
        return isInternalExceptionNoCauses(t.getCause());
    }

    private boolean isInternalExceptionNoCauses(Throwable t)
    {
        // for now assume any exception an internal exception related to buffer service
        // if we see false positives we can filter out stuff here
        return true;
    }
}
