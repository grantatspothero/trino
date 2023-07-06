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
import com.google.inject.Inject;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.Authenticator;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.util.Optional;

import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.RequestBodyHashing;
import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.extractToken;
import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.loadPublicKey;
import static jakarta.ws.rs.HttpMethod.POST;
import static jakarta.ws.rs.HttpMethod.PUT;

public class GalaxyTrinoAuthenticator
        implements Authenticator
{
    private final GalaxyAuthenticatorController controller;

    @Inject
    public GalaxyTrinoAuthenticator(GalaxyTrinoAuthenticatorConfig config)
            throws GeneralSecurityException, IOException
    {
        this(config.getTokenIssuer(), config.getAccountId(), config.getDeploymentId(), loadPublicKey(Optional.ofNullable(config.getPublicKey()), Optional.ofNullable(config.getPublicKeyFile())));
    }

    @VisibleForTesting
    public GalaxyTrinoAuthenticator(String issuer, String accountId, String deploymentId, PublicKey publicKey)
    {
        controller = new GalaxyAuthenticatorController(issuer, Optional.of(accountId), deploymentId, publicKey);
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        if (request.getMethod().equals(POST) && request.getUriInfo().getRequestUri().getPath().startsWith("/v1/statement")) {
            throw new AuthenticationException("Deprecated API", "Galaxy");
        }

        Optional<RequestBodyHashing> requestBodyHashing;
        if (request.getMethod().equals(PUT) && request.getUriInfo().getRequestUri().getPath().startsWith("/v1/statement")) {
            requestBodyHashing = Optional.of(new RequestBodyHashing(request, "statement_hash"));
        }
        else {
            requestBodyHashing = Optional.empty();
        }
        return authenticate(extractToken(request).orElseThrow(() -> new AuthenticationException("Galaxy token is required", "Galaxy")), requestBodyHashing);
    }

    @VisibleForTesting
    public Identity authenticate(String token, Optional<RequestBodyHashing> requestBodyHashing)
            throws AuthenticationException
    {
        return controller.authenticate(token, requestBodyHashing);
    }
}
