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
package io.trino.connector.informationschema.galaxy;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record GalaxyCacheStats(Map<String, Map<String, Stat>> stats)
{
    public GalaxyCacheStats
    {
        checkArgument(stats == null, "stats must be null");
        stats = new TreeMap<>();
    }

    @Inject
    public GalaxyCacheStats()
    {
        this(null);
    }

    public Collection<Stat> get(String catalogName)
    {
        return getStats(catalogName).values();
    }

    public void increment(String catalogName, String name)
    {
        Stat stat = getStat(catalogName, name);
        stat.count.incrementAndGet();
        stat.last.set(Optional.of(Instant.now()));
    }

    public record Stat(String name, AtomicLong count, AtomicReference<Optional<Instant>> last)
    {
        public Stat
        {
            requireNonNull(name, "type is null");
            requireNonNull(count, "count is null");
            requireNonNull(last, "last is null");
        }
    }

    @VisibleForTesting
    Map<String, Stat> getStats(String catalogName)
    {
        return stats.computeIfAbsent(catalogName, ignore -> {
            ConcurrentSkipListMap<String, Stat> map = new ConcurrentSkipListMap<>();
            Stream.of(GalaxyCacheEndpoint.values()).forEach(verb -> {
                initStat(map, verb.successStatName());
                initStat(map, verb.failureStatName());
            });
            return map;
        });
    }

    private Stat initStat(Map<String, Stat> statMap, String name)
    {
        return statMap.computeIfAbsent(name, ignore -> new Stat(name, new AtomicLong(), new AtomicReference<>(Optional.empty())));
    }

    private Stat getStat(String catalogName, String name)
    {
        Map<String, Stat> statMap = getStats(catalogName);
        return initStat(statMap, name);
    }
}
