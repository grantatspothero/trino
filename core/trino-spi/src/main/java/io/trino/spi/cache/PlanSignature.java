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
package io.trino.spi.cache;

import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

/**
 * Plan signature is a normalized and canonicalized representation of subplan.
 * Plan signatures allow to identify, match and adapt similar subqueries.
 * Concept of plan signatures is described in http://www.cs.columbia.edu/~jrzhou/pub/cse.pdf
 */
public class PlanSignature
{
    /**
     * Key of a plan signature. Plans that can be potentially adapted
     * to produce the same results (e.g. using column pruning, filtering or aggregation)
     * will share the same key.
     */
    private final SignatureKey key;
    /**
     * List of group by columns if plan signature represents aggregation.
     */
    private final Optional<List<ColumnId>> groupByColumns;
    /**
     * List of output columns.
     */
    private final List<ColumnId> columns;
    /**
     * Predicate that is satisfied by result set represented by {@link PlanSignature}.
     */
    private final TupleDomain<ColumnId> predicate;

    public PlanSignature(
            SignatureKey key,
            Optional<List<ColumnId>> groupByColumns,
            List<ColumnId> columns,
            TupleDomain<ColumnId> predicate)
    {
        this.key = requireNonNull(key, "key is null");
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    public SignatureKey getKey()
    {
        return key;
    }

    public Optional<List<ColumnId>> getGroupByColumns()
    {
        return groupByColumns;
    }

    public List<ColumnId> getColumns()
    {
        return columns;
    }

    public TupleDomain<ColumnId> getPredicate()
    {
        return predicate;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanSignature signature = (PlanSignature) o;
        return key.equals(signature.key)
                && groupByColumns.equals(signature.groupByColumns)
                && columns.equals(signature.columns)
                && predicate.equals(signature.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(key, groupByColumns, columns, predicate);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", PlanSignature.class.getSimpleName() + "[", "]")
                .add("key=" + key)
                .add("groupByColumns=" + groupByColumns)
                .add("columns=" + columns)
                .add("predicate=" + predicate)
                .toString();
    }
}
