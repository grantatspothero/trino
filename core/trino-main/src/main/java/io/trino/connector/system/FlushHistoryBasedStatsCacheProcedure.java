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
package io.trino.connector.system;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.cost.HistoryBasedStatsCalculator;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;

import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class FlushHistoryBasedStatsCacheProcedure
        implements Provider<Procedure>
{
    private static final String PROCEDURE_NAME = "flush_history_based_stats_cache";

    private static final MethodHandle FLUSH_HISTORY_STATS_CACHE;

    static {
        try {
            FLUSH_HISTORY_STATS_CACHE = lookup().unreflect(FlushHistoryBasedStatsCacheProcedure.class.getMethod("flushHistoryBasedStatsCache"));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final HistoryBasedStatsCalculator historyBasedStatsCalculator;

    @Inject
    public FlushHistoryBasedStatsCacheProcedure(HistoryBasedStatsCalculator historyBasedStatsCalculator)
    {
        this.historyBasedStatsCalculator = requireNonNull(historyBasedStatsCalculator, "historyBasedStatsCalculator is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                PROCEDURE_NAME,
                ImmutableList.of(),
                FLUSH_HISTORY_STATS_CACHE.bindTo(this),
                true);
    }

    public void flushHistoryBasedStatsCache()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            historyBasedStatsCalculator.invalidateCache();
        }
    }
}
