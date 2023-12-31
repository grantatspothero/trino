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
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class GalaxyAccessControlConfig
{
    private URI accountUri;
    private Optional<URI> accessControlOverrideUri = Optional.empty();
    private BiMap<String, CatalogId> catalogNames = ImmutableBiMap.of();
    private Set<String> readOnlyCatalogs = ImmutableSet.of();
    private Map<String, SharedSchemaNameAndAccepted> sharedCatalogSchemaNames = ImmutableMap.of();

    @NotNull
    public URI getAccountUri()
    {
        return accountUri;
    }

    @Config("galaxy.account-url")
    public GalaxyAccessControlConfig setAccountUri(URI accountUri)
    {
        this.accountUri = accountUri;
        return this;
    }

    @NotNull
    public Optional<URI> getAccessControlOverrideUri()
    {
        return accessControlOverrideUri;
    }

    @Config("galaxy.access-control-url")
    public GalaxyAccessControlConfig setAccessControlOverrideUri(URI accessControlOverrideUri)
    {
        this.accessControlOverrideUri = Optional.ofNullable(accessControlOverrideUri);
        return this;
    }

    @NotNull
    public BiMap<String, CatalogId> getCatalogNames()
    {
        return catalogNames;
    }

    public GalaxyAccessControlConfig setCatalogNames(Map<String, CatalogId> catalogNames)
    {
        this.catalogNames = ImmutableBiMap.copyOf(catalogNames);
        return this;
    }

    @Config("galaxy.catalog-names")
    public GalaxyAccessControlConfig setCatalogNames(String catalogNames)
    {
        this.catalogNames = Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("->").split(catalogNames).entrySet().stream()
                .collect(ImmutableBiMap.toImmutableBiMap(Entry::getKey, entry -> new CatalogId(entry.getValue())));
        return this;
    }

    @NotNull
    public Set<String> getReadOnlyCatalogs()
    {
        return readOnlyCatalogs;
    }

    public GalaxyAccessControlConfig setReadOnlyCatalogs(Set<String> readOnlyCatalogs)
    {
        this.readOnlyCatalogs = ImmutableSet.copyOf(readOnlyCatalogs);
        return this;
    }

    @Config("galaxy.read-only-catalogs")
    public GalaxyAccessControlConfig setReadOnlyCatalogs(String catalogNames)
    {
        this.readOnlyCatalogs = Splitter.on(",").trimResults().omitEmptyStrings().splitToStream(catalogNames)
                .collect(toImmutableSet());
        return this;
    }

    @NotNull
    public Map<String, SharedSchemaNameAndAccepted> getSharedCatalogSchemaNames()
    {
        return sharedCatalogSchemaNames;
    }

    public GalaxyAccessControlConfig setSharedCatalogSchemaNames(Map<String, SharedSchemaNameAndAccepted> sharedCatalogSchemaNames)
    {
        this.sharedCatalogSchemaNames = ImmutableMap.copyOf(sharedCatalogSchemaNames);
        return this;
    }

    @Config("galaxy.shared-catalog-schemas")
    public GalaxyAccessControlConfig setSharedCatalogSchemaNames(String sharedCatalogSchemaNames)
    {
        Map<String, String> splitStrings = ImmutableMap.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("->").split(sharedCatalogSchemaNames));
        ImmutableMap.Builder<String, SharedSchemaNameAndAccepted> builder = ImmutableMap.builder();
        splitStrings.forEach((catalogName, value) -> builder.put(catalogName, decodeSharedSchemaString(value)));
        this.sharedCatalogSchemaNames = builder.buildOrThrow();
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
