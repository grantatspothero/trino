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

import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.REFRESH_STATUS_FAILURE;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.REFRESH_STATUS_SUCCESS;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.STAT_CACHE_FAILURE;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.STAT_CACHE_SUCCESS;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.STAT_STATUS_FAILURE;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.STAT_STATUS_SUCCESS;
import static java.util.Objects.requireNonNull;

enum GalaxyCacheEndpoint
{
    ENDPOINT_SCHEMAS("schemas", STAT_CACHE_SUCCESS, STAT_CACHE_FAILURE),
    ENDPOINT_TABLES("tables", STAT_CACHE_SUCCESS, STAT_CACHE_FAILURE),
    ENDPOINT_COLUMNS("columns", STAT_CACHE_SUCCESS, STAT_CACHE_FAILURE),
    ENDPOINT_VIEWS("views", STAT_CACHE_SUCCESS, STAT_CACHE_FAILURE),
    ENDPOINT_STATUS("status", STAT_STATUS_SUCCESS, STAT_STATUS_FAILURE),
    ENDPOINT_REFRESH_CACHE("refreshCache", "POST", REFRESH_STATUS_SUCCESS, REFRESH_STATUS_FAILURE),
    ENDPOINT_MATERIALIZED_VIEWS("materializedViews", STAT_CACHE_SUCCESS, STAT_CACHE_FAILURE);

    private final String verb;
    private final String httpMethod;
    private final String successStatName;
    private final String failureStatName;

    String verbName()
    {
        return verb;
    }

    String httpMethod()
    {
        return httpMethod;
    }

    String successStatName()
    {
        return successStatName;
    }

    String failureStatName()
    {
        return failureStatName;
    }

    GalaxyCacheEndpoint(String verb, String successStatName, String failureStatName)
    {
        this(verb, "GET", successStatName, failureStatName);
    }

    GalaxyCacheEndpoint(String verb, String httpMethod, String successStatName, String failureStatName)
    {
        this.verb = requireNonNull(verb, "verb is null");
        this.httpMethod = requireNonNull(httpMethod, "httpMethod is null");
        this.successStatName = requireNonNull(successStatName, "successStatName is null");
        this.failureStatName = requireNonNull(failureStatName, "failureStatName is null");
    }
}
