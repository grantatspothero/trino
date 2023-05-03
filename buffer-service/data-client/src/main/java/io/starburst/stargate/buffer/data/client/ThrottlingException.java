/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

public class ThrottlingException
        extends DataApiException
{
    private final long nextRequestBaseDelayInMillis;

    public ThrottlingException(ErrorCode errorCode, String message, long nextRequestBaseDelayInMillis)
    {
        super(errorCode, message);
        this.nextRequestBaseDelayInMillis = nextRequestBaseDelayInMillis;
    }

    public long getNextRequestBaseDelayInMillis()
    {
        return nextRequestBaseDelayInMillis;
    }
}
