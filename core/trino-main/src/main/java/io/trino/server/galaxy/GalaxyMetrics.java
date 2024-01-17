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
package io.trino.server.galaxy;

import com.google.inject.Inject;
import io.airlift.stats.CounterStat;
import io.trino.dispatcher.DispatchManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.util.Objects.requireNonNull;

public class GalaxyMetrics
{
    private final DispatchManager dispatchManager;

    @Inject
    public GalaxyMetrics(DispatchManager dispatchManager)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
    }

    @Managed
    public long getQueuedQueries()
    {
        return dispatchManager.getQueuedQueries();
    }

    @Managed
    public long getRunningQueries()
    {
        return dispatchManager.getRunningQueries();
    }

    @Managed
    @Nested
    public CounterStat getCompletedQueries()
    {
        return dispatchManager.getStats().getCompletedQueries();
    }

    @Managed
    @Nested
    public CounterStat getFailedQueries()
    {
        return dispatchManager.getStats().getFailedQueries();
    }

    @Managed
    @Nested
    public CounterStat getUserErrorFailures()
    {
        return dispatchManager.getStats().getUserErrorFailures();
    }

    @Managed
    @Nested
    public CounterStat getInternalFailures()
    {
        return dispatchManager.getStats().getInternalFailures();
    }

    @Managed
    @Nested
    public CounterStat getAbandonedQueries()
    {
        return dispatchManager.getStats().getAbandonedQueries();
    }

    @Managed
    @Nested
    public CounterStat getCanceledQueries()
    {
        return dispatchManager.getStats().getCanceledQueries();
    }
}
