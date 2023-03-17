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

package io.trino.server.resultscache;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class ResultsCacheSessionProperties
        implements SystemSessionPropertiesProvider
{
    private static final String CACHE_KEY = "galaxy_results_cache_key";
    private static final String CACHE_TTL = "galaxy_results_cache_ttl";
    private static final String CACHE_ENTRY_MAX_SIZE_BYTES = "galaxy_results_cache_entry_max_size_bytes";

    private static final List<PropertyMetadata<?>> sessionProperties = ImmutableList.of(
            stringProperty(
                    CACHE_KEY,
                    "Unique key to identify the cache entry",
                    null,
                    true),
            durationProperty(
                    CACHE_TTL,
                    "TTL for the cache entry",
                    null,
                    true),
            longProperty(
                    CACHE_ENTRY_MAX_SIZE_BYTES,
                    "Maximum size for a cache entry in bytes",
                    null,
                    true));

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Optional<String> getResultsCacheKey(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(CACHE_KEY, String.class));
    }

    public static Optional<Duration> getResultsCacheTtl(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(CACHE_TTL, Duration.class));
    }

    public static Optional<Long> getResultsCacheEntryMaxSizeBytes(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(CACHE_ENTRY_MAX_SIZE_BYTES, Long.class));
    }
}
