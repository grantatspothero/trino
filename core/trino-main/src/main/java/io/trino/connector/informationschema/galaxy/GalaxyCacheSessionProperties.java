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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.regex.Pattern;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class GalaxyCacheSessionProperties
        implements SystemSessionPropertiesProvider
{
    public static final String ENABLED = "galaxy_metadata_cache_enabled";
    public static final String MINIMUM_AGE = "galaxy_metadata_cache_minimum_age";
    public static final String MAXIMUM_AGE = "galaxy_metadata_cache_maximum_age";
    public static final String CATALOGS_REGEX = "galaxy_metadata_cache_catalogs_regex";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public GalaxyCacheSessionProperties(GalaxyCacheConfig config)
    {
        sessionProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        CATALOGS_REGEX,
                        "A regular expression that is used to determine which catalogs the cache applies to",
                        VARCHAR,
                        Pattern.class,
                        config.getSessionDefaultCatalogsRegexAsPattern(),
                        false,
                        object -> {
                            String value = (String) object;
                            return Pattern.compile(value);
                        },
                        object -> object),

                booleanProperty(
                        ENABLED,
                        "Enable the Galaxy information_schema cache",
                        config.getSessionDefaultEnabled(),
                        false),

                /*
                    It can take the remote cache a small amount of time to publish changes (i.e. it's eventually consistent). If Galaxy indexes a catalog
                    for the first time it can take a minute or so for the index to be visible to searches. Thus, the min age. Don't use indexes that are too young.
                    Noting this as a comment for other dev's reference. This shouldn't necessarily be communicated to users in the property description
                 */
                durationProperty(
                        MINIMUM_AGE,
                        "The minimum age for the Galaxy information_schema cache to be considered active",
                        config.getSessionDefaultMinimumIndexAge(),
                        false),

                durationProperty(
                        MAXIMUM_AGE,
                        "The maximum age for the Galaxy information_schema cache to be considered active",
                        config.getSessionDefaultMaximumIndexAge(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Pattern getCacheCatalogsRegex(Session session)
    {
        return session.getSystemProperty(CATALOGS_REGEX, Pattern.class);
    }

    public static boolean isEnabled(Session session)
    {
        return session.getSystemProperty(ENABLED, Boolean.class);
    }

    public static Duration minimumAge(Session session)
    {
        return session.getSystemProperty(MINIMUM_AGE, Duration.class);
    }

    public static Duration maximumAge(Session session)
    {
        return session.getSystemProperty(MAXIMUM_AGE, Duration.class);
    }
}
