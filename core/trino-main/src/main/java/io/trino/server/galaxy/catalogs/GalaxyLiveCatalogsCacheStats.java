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
package io.trino.server.galaxy.catalogs;

import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class GalaxyLiveCatalogsCacheStats
{
    private final ConcurrentMap<?, ?> galaxyLiveCatalogsCache;
    private final CounterStat cacheHit = new CounterStat();
    private final CounterStat cacheMiss = new CounterStat();
    private final CounterStat catalogExpired = new CounterStat();

    public GalaxyLiveCatalogsCacheStats(ConcurrentMap<?, ?> galaxyLiveCatalogsCache)
    {
        this.galaxyLiveCatalogsCache = requireNonNull(galaxyLiveCatalogsCache, "galaxyLiveCatalogsCache is null");
    }

    public void recordCacheHit()
    {
        cacheHit.update(1);
    }

    public void recordCacheMiss()
    {
        cacheMiss.update(1);
    }

    public void recordCachedCatalogExpired()
    {
        catalogExpired.update(1);
    }

    @Managed
    public int getGalaxyLiveCatalogsCacheSize()
    {
        return galaxyLiveCatalogsCache.size();
    }

    @Managed
    public double getCacheHitRate()
    {
        long cacheHits = cacheHit.getTotalCount();
        long requestCount = noOverflowAdd(cacheHits, cacheMiss.getTotalCount());
        return (requestCount == 0) ? 1.0 : (double) cacheHits / requestCount;
    }

    @Managed
    public double getCacheRequestCount()
    {
        return noOverflowAdd(cacheHit.getTotalCount(), cacheMiss.getTotalCount());
    }

    private long noOverflowAdd(long left, long right)
    {
        try {
            return Math.addExact((int) left, (int) right);
        }
        catch (ArithmeticException ignored) {
            return Integer.MAX_VALUE;
        }
    }

    @Managed
    @Nested
    public CounterStat getCacheHits()
    {
        return cacheHit;
    }

    @Managed
    @Nested
    public CounterStat getCacheMiss()
    {
        return cacheMiss;
    }

    @Managed
    @Nested
    public CounterStat getCatalogExpired()
    {
        return catalogExpired;
    }
}
