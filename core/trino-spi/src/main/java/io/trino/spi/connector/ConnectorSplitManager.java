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
package io.trino.spi.connector;

import io.trino.spi.Experimental;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;

import java.util.Optional;

public interface ConnectorSplitManager
{
    default ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        throw new UnsupportedOperationException();
    }

    @Experimental(eta = "2023-07-31")
    default ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a unique split identifier. {@link CacheSplitId} should represent raw data
     * (e.g. tuple of filename and file region). {@link CacheSplitId} together with
     * {@link CacheTableId} uniquely represents transformations (e.g. filters, aggregations, projections)
     * that are performed in the context of a split. Same {@link CacheSplitId} can be
     * used with different {@link CacheTableId}.
     */
    default Optional<CacheSplitId> getCacheSplitId(ConnectorSplit split)
    {
        return Optional.empty();
    }
}
