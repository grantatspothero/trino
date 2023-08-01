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
package io.trino.connector.informationschema.galaxy;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class GalaxyCacheConfig
{
    private boolean enabled;
    private boolean sessionDefaultEnabled;
    private Duration sessionDefaultMinimumIndexAge = new Duration(1, TimeUnit.MINUTES);
    private Duration sessionDefaultMaximumIndexAge = new Duration(1.5, TimeUnit.DAYS);
    private Pattern sessionDefaultCatalogsRegex = Pattern.compile(".*");

    public boolean getEnabled()
    {
        return enabled;
    }

    @Config("galaxy.metadata-cache.enabled")
    public GalaxyCacheConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public boolean getSessionDefaultEnabled()
    {
        return sessionDefaultEnabled;
    }

    @Config("galaxy.metadata-cache.session-default.enabled")
    public GalaxyCacheConfig setSessionDefaultEnabled(boolean sessionDefaultEnabled)
    {
        this.sessionDefaultEnabled = sessionDefaultEnabled;
        return this;
    }

    @MinDuration("0s")
    public Duration getSessionDefaultMinimumIndexAge()
    {
        return sessionDefaultMinimumIndexAge;
    }

    @Config("galaxy.metadata-cache.session-default.minimum-age")
    public GalaxyCacheConfig setSessionDefaultMinimumIndexAge(Duration sessionDefaultMinimumIndexAge)
    {
        this.sessionDefaultMinimumIndexAge = sessionDefaultMinimumIndexAge;
        return this;
    }

    @MinDuration("1ms")
    public Duration getSessionDefaultMaximumIndexAge()
    {
        return sessionDefaultMaximumIndexAge;
    }

    @Config("galaxy.metadata-cache.session-default.maximum-age")
    public GalaxyCacheConfig setSessionDefaultMaximumIndexAge(Duration sessionDefaultMaximumIndexAge)
    {
        this.sessionDefaultMaximumIndexAge = sessionDefaultMaximumIndexAge;
        return this;
    }

    // needed to allow TestGalaxyCacheConfig to work properly
    @VisibleForTesting
    public String getSessionDefaultCatalogsRegex()
    {
        return sessionDefaultCatalogsRegex.toString();
    }

    public Pattern getSessionDefaultCatalogsRegexAsPattern()
    {
        return sessionDefaultCatalogsRegex;
    }

    @Config("galaxy.metadata-cache.session-default.catalogs-regex")
    public GalaxyCacheConfig setSessionDefaultCatalogsRegex(String sessionDefaultCatalogsRegex)
    {
        this.sessionDefaultCatalogsRegex = Pattern.compile(requireNonNull(sessionDefaultCatalogsRegex, "catalogsRegex is null"));
        return this;
    }
}
