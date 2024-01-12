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
package io.trino.server.galaxy.autoscaling;

import com.google.common.base.Ticker;
import io.airlift.stats.DecayCounter;
import io.trino.spi.QueryId;

import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class CpuMillisPerWorkerCounter
{
    private Map<QueryId, Long> prevQueryIdToInfo = Collections.emptyMap();
    private final DecayCounter totalPerWorkerCpuMillis;

    public CpuMillisPerWorkerCounter(double alpha, Ticker ticker)
    {
        this.totalPerWorkerCpuMillis = new DecayCounter(alpha, requireNonNull(ticker, "ticker is null"));
    }

    public void update(Map<QueryId, Long> queryIdToQueryCpuTimeMillis, long workers)
    {
        queryIdToQueryCpuTimeMillis.forEach((id, totalQueryCpuMillis) -> {
            long deltaQueryCpuTimePerWorker = getDeltaCpuTime(id, totalQueryCpuMillis) / workers;
            totalPerWorkerCpuMillis.add(deltaQueryCpuTimePerWorker);
        });
        prevQueryIdToInfo = queryIdToQueryCpuTimeMillis;
    }

    private long getDeltaCpuTime(QueryId queryId, long totalQueryCpuTimeMillis)
    {
        long prevCpuTimeMillis = prevQueryIdToInfo.getOrDefault(queryId, 0L);
        return totalQueryCpuTimeMillis - prevCpuTimeMillis;
    }

    public double getCount()
    {
        return totalPerWorkerCpuMillis.getCount();
    }
}
