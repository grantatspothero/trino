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

import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;

public class MetadataOnlySystemState
{
    private static final Logger log = Logger.get(MetadataOnlySystemState.class);

    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final AtomicInteger activeRequests = new AtomicInteger();
    private final Duration exitCheckDelay;

    @Inject
    public MetadataOnlySystemState(MetadataOnlyConfig config)
    {
        exitCheckDelay = config.getShutdownExitCheckDelay();
    }

    public void setShuttingDown()
    {
        // this will prevent new requests from being processed
        shuttingDown.set(true);

        // there may be no active requests, so we should shut down immediately
        // however, there is a race if new requests come in around the same time
        // as this method is called. Thus, schedule the exit check a few seconds
        // into the future
        Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("MetadataOnlySystemStateShutdown"))
                        .schedule(this::exitIfNeeded, exitCheckDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    public boolean isShuttingDown()
    {
        return shuttingDown.get();
    }

    public void incrementActiveRequests()
    {
        activeRequests.incrementAndGet();
    }

    public void decrementAndGetActiveRequests()
    {
        int newCount = activeRequests.decrementAndGet();
        checkState(newCount >= 0, "activeRequests has gone negative: " + newCount);

        exitIfNeeded();
    }

    private void exitIfNeeded()
    {
        if ((activeRequests.get() <= 0) && isShuttingDown()) {
            log.info("Shutdown requested and there are no active requests. Exiting.");
            System.exit(0);
        }
    }
}
