/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

public class TestChunkManagerSpoolMergeEnabled
        extends AbstractTestChunkManager
{
    @Override
    protected boolean isChunkSpoolMergeEnabled()
    {
        return true;
    }
}
