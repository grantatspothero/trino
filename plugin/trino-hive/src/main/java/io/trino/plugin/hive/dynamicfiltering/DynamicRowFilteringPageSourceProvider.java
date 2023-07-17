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
package io.trino.plugin.hive.dynamicfiltering;

import com.google.inject.Inject;
import com.starburstdata.trino.plugins.dynamicfiltering.DynamicPageFilterCache;
import com.starburstdata.trino.plugins.dynamicfiltering.DynamicRowFilteringPageSource;
import com.starburstdata.trino.plugins.dynamicfiltering.ForDynamicRowFiltering;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;

import java.util.List;

import static com.starburstdata.trino.plugins.dynamicfiltering.DynamicRowFilteringSessionProperties.getDynamicRowFilterSelectivityThreshold;
import static com.starburstdata.trino.plugins.dynamicfiltering.DynamicRowFilteringSessionProperties.getDynamicRowFilteringWaitTimeout;
import static com.starburstdata.trino.plugins.dynamicfiltering.DynamicRowFilteringSessionProperties.isDynamicRowFilteringEnabled;
import static java.util.Objects.requireNonNull;

/**
 * Copy of DynamicRowFilteringModule from Starburst Trino plugins repo because adjustments
 * to {@link ConnectorPageSourceProvider} are required for Galaxy SPI.
 */
public class DynamicRowFilteringPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ConnectorPageSourceProvider delegatePageSourceProvider;
    private final DynamicPageFilterCache dynamicPageFilterCache;

    @Inject
    public DynamicRowFilteringPageSourceProvider(
            @ForDynamicRowFiltering ConnectorPageSourceProvider delegatePageSourceProvider,
            DynamicPageFilterCache dynamicPageFilterCache)
    {
        this.delegatePageSourceProvider = requireNonNull(delegatePageSourceProvider, "delegatePageSourceProvider is null");
        this.dynamicPageFilterCache = requireNonNull(dynamicPageFilterCache, "dynamicPageFilterCollector is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        ConnectorPageSource pageSource = delegatePageSourceProvider.createPageSource(transaction, session, split, tableHandle, columns, dynamicFilter);
        if (dynamicFilter == DynamicFilter.EMPTY
                || !isDynamicRowFilteringEnabled(session)
                || pageSource instanceof RecordPageSource) {
            return pageSource;
        }
        if (dynamicFilter.isComplete() && dynamicFilter.getCurrentPredicate().isAll()) {
            return pageSource;
        }

        return new DynamicRowFilteringPageSource(
                pageSource,
                getDynamicRowFilterSelectivityThreshold(session),
                getDynamicRowFilteringWaitTimeout(session),
                columns,
                dynamicPageFilterCache.getDynamicPageFilter(dynamicFilter, columns));
    }
}
