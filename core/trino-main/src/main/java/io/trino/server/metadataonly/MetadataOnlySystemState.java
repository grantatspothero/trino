/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.metadataonly;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;

public class MetadataOnlySystemState
{
    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final AtomicInteger activeRequests = new AtomicInteger();

    public void setShuttingDown()
    {
        shuttingDown.set(true);
    }

    public boolean isShuttingDown()
    {
        return shuttingDown.get();
    }

    public void incrementActiveRequests()
    {
        activeRequests.incrementAndGet();
    }

    public int decrementAndGetActiveRequests()
    {
        int newCount = activeRequests.decrementAndGet();
        checkState(newCount >= 0, "activeRequests has gone negative: " + newCount);
        return newCount;
    }
}
