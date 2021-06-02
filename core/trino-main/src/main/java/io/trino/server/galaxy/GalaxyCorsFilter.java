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
package io.trino.server.galaxy;

import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.ext.Provider;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Provider
@PreMatching
public class GalaxyCorsFilter
        implements ContainerRequestFilter, ContainerResponseFilter
{
    // If you change this, also update the CORS filter in dispatcher
    private static final List<String> TRINO_REQUEST_HEADERS = ImmutableList.<String>builder()
            .add("X-Trino-User")
            .add("X-Trino-Source")
            .add("X-Trino-Catalog")
            .add("X-Trino-Schema")
            .add("X-Trino-Path")
            .add("X-Trino-Time-Zone")
            .add("X-Trino-Language")
            .add("X-Trino-Trace-Token")
            .add("X-Trino-Session")
            .add("X-Trino-Role")
            .add("X-Trino-Prepared-Statement")
            .add("X-Trino-Transaction-Id")
            .add("X-Trino-Client-Info")
            .add("X-Trino-Client-Tags")
            .add("X-Trino-Client-Capabilities")
            .add("X-Trino-Resource-Estimate")
            .add("X-Trino-Extra-Credential")
            .build();
    private static final String ALLOWED_REQUEST_HEADERS = "origin, content-type, accept, authorization, " + String.join(", ", TRINO_REQUEST_HEADERS);

    // If you change this, also update the CORS filter in dispatcher
    private static final List<String> TRINO_RESPONSE_HEADERS = ImmutableList.<String>builder()
            .add("X-Trino-Set-Catalog")
            .add("X-Trino-Set-Schema")
            .add("X-Trino-Set-Path")
            .add("X-Trino-Set-Session")
            .add("X-Trino-Clear-Session")
            .add("X-Trino-Set-Role")
            .build();
    private static final String ALLOWED_RESPONSE_HEADERS = String.join(", ", TRINO_RESPONSE_HEADERS);

    private final String allowedOriginSuffix;

    @Inject
    public GalaxyCorsFilter(GalaxyCorsConfig config)
    {
        this(config.getCorsAllowedBaseDomain());
    }

    public GalaxyCorsFilter(String allowedBaseDomain)
    {
        allowedOriginSuffix = requireNonNull(allowedBaseDomain, "allowedBaseDomain is null").trim();
        checkArgument(!allowedOriginSuffix.isBlank(), "allowedBaseDomain is blank");
    }

    @Override
    public void filter(ContainerRequestContext requestContext)
    {
        String origin = requestContext.getHeaderString("Origin");
        // is this a CORS preflight check
        if (origin != null && requestContext.getMethod().equalsIgnoreCase("OPTIONS")) {
            ResponseBuilder builder = Response.ok();
            addCorsHeaders(origin, builder.build().getHeaders());
            requestContext.abortWith(builder.build());
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
    {
        addCorsHeaders(requestContext.getHeaderString("Origin"), responseContext.getHeaders());
    }

    private void addCorsHeaders(String origin, MultivaluedMap<String, Object> headers)
    {
        if (origin != null && isAllowedOrigin(origin)) {
            headers.putSingle("Access-Control-Allow-Origin", origin);
            headers.putSingle("Access-Control-Allow-Credentials", "true");
            headers.putSingle("Access-Control-Allow-Headers", ALLOWED_REQUEST_HEADERS);
            headers.putSingle("Access-Control-Expose-Headers", ALLOWED_RESPONSE_HEADERS);
            headers.putSingle("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
            headers.putSingle("Access-Control-Max-Age", "86400");
        }
    }

    private boolean isAllowedOrigin(String origin)
    {
        try {
            URI originUri = new URI(origin);
            return allowedOriginSuffix.equals("*") ||
                    originUri.getHost() != null && (
                            originUri.getHost().equals(allowedOriginSuffix) ||
                            originUri.getHost().endsWith("." + allowedOriginSuffix));
        }
        catch (URISyntaxException e) {
            return false;
        }
    }
}
