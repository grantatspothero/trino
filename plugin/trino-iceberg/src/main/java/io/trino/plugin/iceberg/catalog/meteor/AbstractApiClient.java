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
package io.trino.plugin.iceberg.catalog.meteor;

import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.json.JsonCodec;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.Arrays;
import java.util.Optional;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

// Copied from io.starburst.stargate.portal.server.AbstractApiClient
public abstract class AbstractApiClient
{
    private final URI baseUrl;
    private final String basePath;
    private final HttpClient client;

    public AbstractApiClient(HttpClient client, URI baseUrl, String basePath)
    {
        this.client = requireNonNull(client, "client is null");
        this.baseUrl = requireNonNull(baseUrl, "baseUrl is null");
        this.basePath = requireNonNull(basePath, "basePath is null");
    }

    protected URI uriFor(Optional<String> method, Optional<String> queryParam, String... pathEnd)
    {
        if (method.isPresent()) {
            return baseUrl.resolve(basePath + Arrays.stream(pathEnd).collect(joining("/", "/", ":%s".formatted(method.get()))));
        }
        else if (queryParam.isPresent()) {
            return baseUrl.resolve(basePath + Arrays.stream(pathEnd).collect(joining("/", "/", "?%s".formatted(queryParam.get()))));
        }
        else {
            return baseUrl.resolve(basePath + Arrays.stream(pathEnd).collect(joining("/", "/", "")));
        }
    }

    protected <T> T execute(Request request, JsonCodec<T> codec)
    {
        JsonResponse<T> response = client.execute(request, createFullJsonResponseHandler(codec));
        return switch (response.getStatusCode()) {
            case HTTP_OK, HTTP_NO_CONTENT -> response.getValue();
            default -> throw new TrinoException(CATALOG_NOT_AVAILABLE, response.getResponseBody());
        };
    }

    protected Optional<String> execute(Request request)
    {
        StringResponse response = client.execute(request, createStringResponseHandler());
        return switch (response.getStatusCode()) {
            case HTTP_OK, HTTP_NO_CONTENT -> Optional.of(response.getBody());
            case HTTP_NOT_FOUND -> Optional.empty();
            default -> throw new TrinoException(CATALOG_NOT_AVAILABLE, response.getBody());
        };
    }

    protected boolean executeHead(Request request)
    {
        StringResponse response = client.execute(request, createStringResponseHandler());
        return switch (response.getStatusCode()) {
            case HTTP_OK -> true;
            case HTTP_NOT_FOUND -> false;
            default -> throw new RuntimeException("Failed to get a response");
        };
    }
}
