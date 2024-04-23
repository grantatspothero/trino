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
package io.trino.plugin.objectstore;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.starburst.stargate.id.CatalogId;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

public class GalaxySecurityConfig
{
    private boolean enabled = true;
    private URI accountUri;
    private Optional<URI> overrideUri = Optional.empty();
    private Optional<URI> locationOverrideUri = Optional.empty();
    private CatalogId catalogId;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("galaxy.location-security.enabled")
    @ConfigHidden
    public GalaxySecurityConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    public URI getAccountUri()
    {
        return accountUri;
    }

    @CanIgnoreReturnValue
    @Config("galaxy.account-url")
    public GalaxySecurityConfig setAccountUri(URI accountUri)
    {
        this.accountUri = accountUri;
        return this;
    }

    @NotNull
    public Optional<URI> getAccessControlUri()
    {
        return overrideUri;
    }

    @Config("galaxy.access-control-url")
    public GalaxySecurityConfig setAccessControlUri(URI overrideUri)
    {
        this.overrideUri = Optional.ofNullable(overrideUri);
        return this;
    }

    @NotNull
    public Optional<URI> getLocationOverrideUri()
    {
        return locationOverrideUri;
    }

    @Config("galaxy.location-access-control-url")
    @ConfigDescription("""
            An optional URL to use for location access control checks.
            This property may not be present. If not, the galaxy.account-url should be used instead.
            """)
    public GalaxySecurityConfig setLocationOverrideUri(URI locationOverrideUri)
    {
        this.locationOverrideUri = Optional.ofNullable(locationOverrideUri);
        return this;
    }

    @NotNull
    public CatalogId getCatalogId()
    {
        return catalogId;
    }

    @CanIgnoreReturnValue
    @Config("galaxy.catalog-id")
    public GalaxySecurityConfig setCatalogId(CatalogId catalogId)
    {
        this.catalogId = catalogId;
        return this;
    }
}
