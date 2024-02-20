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
package io.trino.server.security.galaxy;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Config for {@link GalaxyAccessControl} subsystem.
 * {@link GalaxyAccessControlConfig} but bound in the main context only (not twice).
 */
public class GalaxySystemAccessControlConfig
{
    public enum FilterColumnsAcceleration
    {
        NONE,
        FCX2,
    }

    private FilterColumnsAcceleration filterColumnsAcceleration = FilterColumnsAcceleration.FCX2;
    private int backgroundProcessingThreads = 8;
    // Currently, we allow at most 60 concurrent queries (20 queries and 40 "data definition"), this value is with some margin.
    private int expectedQueryParallelism = 100;
    private Set<String> readOnlyCatalogs = ImmutableSet.of();
    private Optional<Map<String, SharedSchemaNameAndAccepted>> sharedCatalogSchemaNames = Optional.empty();

    @NotNull
    public FilterColumnsAcceleration getFilterColumnsAcceleration()
    {
        return filterColumnsAcceleration;
    }

    @Config("galaxy.filter-columns-acceleration")
    public GalaxySystemAccessControlConfig setFilterColumnsAcceleration(FilterColumnsAcceleration filterColumnsAcceleration)
    {
        this.filterColumnsAcceleration = filterColumnsAcceleration;
        return this;
    }

    @Min(0)
    public int getBackgroundProcessingThreads()
    {
        return backgroundProcessingThreads;
    }

    @Config("galaxy.access-control-background-threads")
    public GalaxySystemAccessControlConfig setBackgroundProcessingThreads(int backgroundProcessingThreads)
    {
        this.backgroundProcessingThreads = backgroundProcessingThreads;
        return this;
    }

    @Min(1)
    public int getExpectedQueryParallelism()
    {
        return expectedQueryParallelism;
    }

    @Config("galaxy.expected-query-parallelism")
    @ConfigDescription("Expected query parallelism, should be the sum of hardConcurrencyLimit of all resource groups")
    public GalaxySystemAccessControlConfig setExpectedQueryParallelism(int expectedQueryParallelism)
    {
        this.expectedQueryParallelism = expectedQueryParallelism;
        return this;
    }

    @NotNull
    public Set<String> getReadOnlyCatalogs()
    {
        return readOnlyCatalogs;
    }

    public GalaxySystemAccessControlConfig setReadOnlyCatalogs(Set<String> readOnlyCatalogs)
    {
        this.readOnlyCatalogs = ImmutableSet.copyOf(requireNonNull(readOnlyCatalogs));
        return this;
    }

    @Config("galaxy.read-only-catalogs")
    public GalaxySystemAccessControlConfig setReadOnlyCatalogs(String catalogNames)
    {
        this.readOnlyCatalogs = Splitter.on(",").trimResults().omitEmptyStrings().splitToStream(catalogNames)
                .collect(toImmutableSet());
        return this;
    }

    @NotNull
    public Optional<Map<String, SharedSchemaNameAndAccepted>> getSharedCatalogSchemaNames()
    {
        return sharedCatalogSchemaNames;
    }

    public GalaxySystemAccessControlConfig setSharedCatalogSchemaNames(Optional<Map<String, SharedSchemaNameAndAccepted>> sharedCatalogSchemaNames)
    {
        this.sharedCatalogSchemaNames = requireNonNull(sharedCatalogSchemaNames, "sharedCatalogSchemaNames is null");
        return this;
    }

    @Config("galaxy.shared-catalog-schemas")
    public GalaxySystemAccessControlConfig setSharedCatalogSchemaNames(String sharedCatalogSchemaNames)
    {
        if (sharedCatalogSchemaNames.isBlank()) {
            this.sharedCatalogSchemaNames = Optional.empty();
        }
        else {
            Map<String, String> splitStrings = ImmutableMap.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("->").split(sharedCatalogSchemaNames));
            ImmutableMap.Builder<String, SharedSchemaNameAndAccepted> builder = ImmutableMap.builder();
            splitStrings.forEach((catalogName, value) -> builder.put(catalogName, decodeSharedSchemaString(value)));
            this.sharedCatalogSchemaNames = Optional.of(builder.buildOrThrow());
        }
        return this;
    }

    /**
     * The format of the string:
     * schemaName if accepted
     * *schemaName if not accepted and schemaName is non-null
     * * if not accepted and schemaName is null
     */
    private static SharedSchemaNameAndAccepted decodeSharedSchemaString(String value)
    {
        checkArgument(value != null && !value.isEmpty(), "value %s is null or empty", value);
        if ("*".equals(value)) {
            return new SharedSchemaNameAndAccepted(null, false);
        }
        if (value.startsWith("*")) {
            return new SharedSchemaNameAndAccepted(value.substring(1), false);
        }
        return new SharedSchemaNameAndAccepted(value, true);
    }
}
