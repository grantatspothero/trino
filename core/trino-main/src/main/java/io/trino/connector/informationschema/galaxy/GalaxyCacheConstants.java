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

import java.util.function.UnaryOperator;

import static io.trino.connector.informationschema.galaxy.GalaxyCacheSessionProperties.ENABLED;

final class GalaxyCacheConstants
{
    private static final String NAME = "Galaxy metadata cache";

    static final String SCHEMA_NAME = "galaxy_metadata_cache";

    static final String STAT_CACHE_SUCCESS = "cache_hits";
    static final String STAT_CACHE_FAILURE = "cache_misses";
    static final String STAT_STATUS_SUCCESS = "status";
    static final String STAT_STATUS_FAILURE = "status_error";
    static final String REFRESH_STATUS_SUCCESS = "refresh";
    static final String REFRESH_STATUS_FAILURE = "refresh_error";

    static final UnaryOperator<String> ERROR_RECENTLY_REFRESHED = "You've already refreshed \"%s\" recently. Please wait to refresh it."::formatted;
    static final UnaryOperator<String> ERROR_CACHE_IS_UNAVAILABLE = s -> "%s is unavailable for: \"%s\"".formatted(NAME, s);
    static final String ERROR_CACHE_IS_DISABLED = "%s is not enabled. Enable via: SET SESSION %s = true".formatted(NAME, ENABLED);

    private GalaxyCacheConstants() {}
}
