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
import io.airlift.configuration.ConfigHidden;

import javax.validation.constraints.NotEmpty;

import java.net.URI;

public class GalaxyLocationSecurityConfig
{
    private boolean enabled = true;
    private URI accountUri;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("galaxy.location-security.enabled")
    @ConfigHidden
    public GalaxyLocationSecurityConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotEmpty
    public URI getAccountUri()
    {
        return accountUri;
    }

    @CanIgnoreReturnValue
    @Config("galaxy.account-url")
    public GalaxyLocationSecurityConfig setAccountUri(URI accountUri)
    {
        this.accountUri = accountUri;
        return this;
    }
}
