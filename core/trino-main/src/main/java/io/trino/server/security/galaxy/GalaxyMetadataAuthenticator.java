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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.server.metadataonly.MetadataOnlyConfig;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.Authenticator;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.RequestBodyHashing;
import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.extractToken;
import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.loadPublicKey;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.DISPATCH;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.INDEXER;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.PORTAL;
import static jakarta.ws.rs.HttpMethod.POST;

public class GalaxyMetadataAuthenticator
        implements Authenticator
{
    private final GalaxyMetadataAuthenticatorController controller;

    @Inject
    public GalaxyMetadataAuthenticator(GalaxyMetadataAuthenticatorConfig authenticatorConfig, MetadataOnlyConfig metadataConfig)
            throws GeneralSecurityException, IOException
    {
        this(ImmutableMap.<String, Set<String>>builder()
                        .put(authenticatorConfig.getPortalTokenIssuer(), ImmutableSet.of())
                        .put(authenticatorConfig.getDispatchTokenIssuer(), ImmutableSet.of())
                        .buildOrThrow(),
                ImmutableSet.<GalaxyIdentityType>builder()
                        .add(PORTAL)
                        .add(INDEXER)
                        .add(DISPATCH)
                        .build(),
                metadataConfig.getTrinoPlaneId().toString(), loadPublicKey(Optional.ofNullable(authenticatorConfig.getPublicKey()), Optional.ofNullable(authenticatorConfig.getPublicKeyFile())));
    }

    @VisibleForTesting
    public GalaxyMetadataAuthenticator(Map<String, Set<String>> issuerAudienceMapping, Set<GalaxyIdentityType> validIdenityTypes, String trinoPlaneId, PublicKey publicKey)
    {
        this.controller = new GalaxyMetadataAuthenticatorController(issuerAudienceMapping, validIdenityTypes, trinoPlaneId, publicKey);
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        Optional<String> token = extractToken(request);
        Optional<RequestBodyHashing> requestBodyHashing;
        if (request.getMethod().equals(POST) && request.getUriInfo().getRequestUri().getPath().startsWith("/galaxy/metadata/v1/statement")) {
            requestBodyHashing = Optional.of(new RequestBodyHashing(request, "request_hash"));
        }
        else {
            requestBodyHashing = Optional.empty();
        }
        return authenticate(token.orElseThrow(() -> new AuthenticationException("Galaxy token is required", "Galaxy")), requestBodyHashing);
    }

    @VisibleForTesting
    public Identity authenticate(String token, Optional<RequestBodyHashing> requestBodyHashing)
            throws AuthenticationException
    {
        return controller.authenticate(token, requestBodyHashing);
    }
}
