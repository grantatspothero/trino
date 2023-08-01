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
import dev.failsafe.Failsafe;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.server.security.galaxy.GalaxyAccessControlConfig;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Pattern;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.Family.SUCCESSFUL;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.starburst.stargate.exceptions.RetryPolicyMaker.RetryMode.RETRY_ALL;
import static io.starburst.stargate.exceptions.RetryPolicyMaker.createHttpClientRetryPolicy;
import static io.starburst.stargate.identity.DispatchSession.AUTH_TOKEN_TYPE;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.ERROR_CACHE_IS_DISABLED;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.ERROR_CACHE_IS_UNAVAILABLE;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheSessionProperties.getCacheCatalogsRegex;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheSessionProperties.isEnabled;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheSessionProperties.maximumAge;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheSessionProperties.minimumAge;
import static io.trino.server.security.galaxy.GalaxyIdentity.toDispatchSession;
import static jakarta.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class GalaxyCacheClient
{
    private static final JsonCodec<List<List<Object>>> ROWS_CODEC = listJsonCodec(listJsonCodec(Object.class));

    private final HttpClient httpClient;
    private final URI accountUri;
    private final GalaxyCacheStats stats;

    @Inject
    GalaxyCacheClient(@ForGalaxyCache HttpClient httpClient, GalaxyAccessControlConfig accessControlConfig, GalaxyCacheStats stats)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.stats = requireNonNull(stats, "stats is null");
        accountUri = accessControlConfig.getAccountUri();
    }

    Iterator<List<Object>> queryResults(Session session, QualifiedTablePrefix prefix, GalaxyCacheEndpoint verb, URI uri)
    {
        if (!isEnabled(session)) {
            stats.increment(prefix.getCatalogName(), verb.failureStatName());
            throw new NotFoundException(ERROR_CACHE_IS_DISABLED);
        }

        Pattern catalogsRegex = getCacheCatalogsRegex(session);
        if (!catalogsRegex.matcher(prefix.getCatalogName()).matches()) {
            // this is not an error case - user doesn't want caching for this catalog - don't increment the stat
            throw new NotFoundException(ERROR_CACHE_IS_UNAVAILABLE.apply(prefix.getCatalogName()));
        }

        String accessToken = toDispatchSession(session.getIdentity()).getAccessToken();

        Request request = Request.builder()
                .setUri(uri)
                .addHeader(AUTHORIZATION, format("%s %s", AUTH_TOKEN_TYPE, accessToken))
                .setMethod(verb.httpMethod())
                .build();

        return Failsafe.with(createHttpClientRetryPolicy(RETRY_ALL))
                .get(() -> {
                    JsonResponse<List<List<Object>>> response = httpClient.execute(request, createFullJsonResponseHandler(ROWS_CODEC));

                    if (response.getStatusCode() == HttpStatus.NOT_FOUND.code()) {
                        stats.increment(prefix.getCatalogName(), verb.failureStatName());
                        throw new NotFoundException(ERROR_CACHE_IS_UNAVAILABLE.apply(prefix.getCatalogName()));
                    }

                    if (HttpStatus.fromStatusCode(response.getStatusCode()).family() != SUCCESSFUL) {
                        stats.increment(prefix.getCatalogName(), verb.failureStatName());
                        throw new WebApplicationException(Response.status(response.getStatusCode()).build());
                    }

                    List<List<Object>> value = (response.getStatusCode() != HttpStatus.NO_CONTENT.code()) ? response.getValue() : ImmutableList.of();
                    stats.increment(prefix.getCatalogName(), verb.successStatName());
                    return value.iterator();
                });
    }

    HttpUriBuilder uriBuilder(Session session, QualifiedTablePrefix prefix, GalaxyCacheEndpoint verb, OptionalLong limit)
    {
        HttpUriBuilder builder = HttpUriBuilder.uriBuilderFrom(accountUri)
                .replacePath("/api/v1/galaxy/trino/search")
                .appendPath(verb.verbName())
                .appendPath(prefix.getCatalogName())
                .addParameter("minimumIndexAge", convertAirliftDuration(minimumAge(session)))
                .addParameter("maximumIndexAge", convertAirliftDuration(maximumAge(session)));
        prefix.getSchemaName().ifPresent(schemaName -> builder.addParameter("schemaName", schemaName));
        prefix.getTableName().ifPresent(tableName -> builder.addParameter("tableName", tableName));
        limit.ifPresent(l -> builder.addParameter("limit", Long.toString(l)));
        return builder;
    }

    private static String convertAirliftDuration(Duration duration)
    {
        return duration.toJavaTime().toString();
    }
}
