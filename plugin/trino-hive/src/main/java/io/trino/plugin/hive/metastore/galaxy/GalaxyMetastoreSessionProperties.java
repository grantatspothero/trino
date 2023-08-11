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
package io.trino.plugin.hive.metastore.galaxy;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public class GalaxyMetastoreSessionProperties
        implements SessionPropertiesProvider
{
    public static final String METASTORE_STREAM_TABLES_FETCH_SIZE = "metastore_stream_tables_fetch_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    public GalaxyMetastoreSessionProperties()
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(integerProperty(
                        // TODO remove the property, it's for tuning during development only
                        METASTORE_STREAM_TABLES_FETCH_SIZE,
                        "Desired fetch size when paging tables in schema",
                        null,
                        value -> checkArgument(0 < value && value < 10_000, "Invalid value: %s", value),
                        true))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Optional<Integer> getMetastoreStreamTablesFetchSize(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(METASTORE_STREAM_TABLES_FETCH_SIZE, Integer.class));
    }
}
