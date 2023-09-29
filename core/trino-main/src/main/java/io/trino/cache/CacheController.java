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
package io.trino.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.tree.Expression;

import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.isCacheSubqueriesEnabled;

public class CacheController
{
    /**
     * Logic for cache decision (what to cache, order or caching candidates).
     */
    public List<CacheCandidate> getCachingCandidates(Session session, List<CanonicalSubplan> canonicalSubplans)
    {
        if (!isCacheSubqueriesEnabled(session)) {
            return ImmutableList.of();
        }

        return canonicalSubplans.stream()
                .map(subplan -> new SimpleEntry<>(toSubplanKey(subplan), subplan))
                .sorted(Comparator.comparing(entry -> entry.getKey().getPriority()))
                .collect(toImmutableListMultimap(SimpleEntry::getKey, SimpleEntry::getValue))
                .asMap().entrySet().stream()
                .map(entry -> new CacheCandidate(entry.getKey().tableId(), entry.getKey().groupByColumns(), ImmutableList.copyOf(entry.getValue()), 2))
                .collect(toImmutableList());
    }

    record CacheCandidate(CacheTableId tableId, Optional<Set<CacheColumnId>> groupByColumns, List<CanonicalSubplan> subplans, int minSubplans) {}

    private static SubplanKey toSubplanKey(CanonicalSubplan subplan)
    {
        return toSubplanKey(subplan.getTableId(), subplan.getGroupByColumns(), subplan.getConjuncts());
    }

    @VisibleForTesting
    static SubplanKey toSubplanKey(CacheTableId tableId, Optional<Set<CacheColumnId>> groupByColumns, List<Expression> conjuncts)
    {
        if (groupByColumns.isEmpty()) {
            return new SubplanKey(tableId, Optional.empty(), ImmutableSet.of());
        }

        Set<Symbol> groupBySymbols = groupByColumns.get().stream()
                .map(CanonicalSubplanExtractor::columnIdToSymbol)
                .collect(toImmutableSet());

        // extract conjuncts that can't be pulled though group by columns
        Set<Expression> nonPullableConjuncts = conjuncts.stream()
                .filter(expression -> !groupBySymbols.containsAll(SymbolsExtractor.extractAll(expression)))
                .collect(toImmutableSet());

        return new SubplanKey(tableId, groupByColumns, nonPullableConjuncts);
    }

    record SubplanKey(
            CacheTableId tableId,
            Optional<Set<CacheColumnId>> groupByColumns,
            // conjuncts that cannot be pulled up though aggregation
            Set<Expression> nonPullableConjuncts)
    {
        public int getPriority()
        {
            return groupByColumns.isPresent() ? 0 : 1;
        }
    }
}
