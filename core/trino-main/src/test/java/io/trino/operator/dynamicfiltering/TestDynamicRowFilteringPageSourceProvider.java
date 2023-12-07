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
package io.trino.operator.dynamicfiltering;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingMetadata;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingSplit;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicRowFilteringPageSourceProvider
{
    @Test
    public void testFill()
    {
        DynamicRowFilteringPageSourceProvider pageSourceProvider = new DynamicRowFilteringPageSourceProvider(
                new DynamicPageFilterCache(new TypeOperators()));
        PropertyMetadata<Boolean> property = new PropertyMetadata<>(
                "dynamic_row_filtering_enabled",
                "description",
                VARBINARY,
                Boolean.class,
                true,
                false,
                param -> TRUE,
                param -> param);

        List<PropertyMetadata<?>> properties = new ArrayList<>();
        properties.add(property);
        Map<ColumnHandle, NullableValue> values = new HashMap<>();
        values.put(new TestingMetadata.TestingColumnHandle("column"), new NullableValue(BIGINT, 1L));

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(properties)
                .build();
        assertThat(pageSourceProvider.simplifyPredicate(
                new TestPageSourceProvider(),
                TestingSession.testSessionBuilder().build(),
                session,
                new TestingSplit(true, new ArrayList<>()),
                new ConnectorTableHandle() {},
                TupleDomain.fromFixedValues(values))
        ).isEqualTo(TupleDomain.none());
    }

    private static class TestPageSourceProvider
            implements ConnectorPageSourceProvider
    {
        @Override
        public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
        {
            return null;
        }

        @Override
        public TupleDomain<ColumnHandle> simplifyPredicate(ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, TupleDomain<ColumnHandle> predicate)
        {
            return ConnectorPageSourceProvider.super.simplifyPredicate(session, split, table, predicate);
        }

        @Override
        public TupleDomain<ColumnHandle> prunePredicate(ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, TupleDomain<ColumnHandle> predicate)
        {
            return TupleDomain.none();
        }
    }
}
