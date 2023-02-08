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

import io.airlift.log.Logger;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;

import javax.annotation.Priority;
import javax.crypto.spec.SecretKeySpec;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

import java.io.IOException;
import java.security.Key;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.BaseEncoding.base16;
import static io.jsonwebtoken.SignatureAlgorithm.HS256;
import static io.trino.server.security.galaxy.GalaxyAuthenticatorController.parseClaimsWithoutValidation;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;

@Priority(Priorities.AUTHENTICATION)
public class GalaxyOperatorAuthenticationFilter
        implements ContainerRequestFilter
{
    private static final Logger log = Logger.get(GalaxyOperatorAuthenticationFilter.class);
    private static final String OPERATOR_AUTHENTICATION_HEADER = "X-Operator-Authentication";
    private static final String OPERATOR_SHARED_SECRET_HEADER = "X-Operator-Shared-Secret";

    private final JwtParser jwtParser;
    private final String operatorSharedSecret;

    @Inject
    public GalaxyOperatorAuthenticationFilter(GalaxyConfig galaxyConfig, GalaxyOperatorAuthenticationConfig operatorAuthenticationConfig)
    {
        operatorSharedSecret = operatorAuthenticationConfig.getOperatorSharedSecret();
        jwtParser = newJwtParserBuilder()
                .setSigningKey(decodeHmacSha256Key(operatorAuthenticationConfig.getOperatorSharedSecret()))
                .requireIssuer(operatorAuthenticationConfig.getOperatorTokenIssuer())
                .requireAudience(galaxyConfig.getAccountId())
                .requireSubject(galaxyConfig.getDeploymentId())
                .build();
    }

    private static Key decodeHmacSha256Key(String key)
    {
        requireNonNull(key, "key is null");
        checkArgument(key.length() == 64, "key must be 64 hex characters");
        return new SecretKeySpec(base16().decode(key.toUpperCase(US)), HS256.getJcaName());
    }

    @Override
    public void filter(ContainerRequestContext requestContext)
            throws IOException
    {
        String requestPath = requestContext.getUriInfo().getPath();
        if (requestPath.startsWith("v1/statement/drainState") || requestPath.startsWith("v1/galaxy/health")) {
            String authHeader = requestContext.getHeaderString(OPERATOR_AUTHENTICATION_HEADER);
            if (authHeader == null || authHeader.isEmpty()) {
                log.error("Missing or empty authentication header");
                throw new ForbiddenException("Invalid Authentication");
            }
            validateOperatorAuthentication(authHeader);
        }

        if (requestPath.startsWith("v1/galaxy/info")) {
            String envSecret = requestContext.getHeaderString(OPERATOR_SHARED_SECRET_HEADER);
            if (!operatorSharedSecret.equals(envSecret)) {
                log.error("Missing or incorrect shared secret header");
                throw new ForbiddenException("Invalid Authentication");
            }
        }
    }

    private void validateOperatorAuthentication(String token)
    {
        try {
            jwtParser.parseClaimsJws(token);
        }
        catch (JwtException e) {
            log.error(e, "Exception parsing JWT: %s", parseClaimsWithoutValidation(token).orElse("not a JWT"));
            throw new ForbiddenException("Invalid Authentication", e);
        }
        catch (RuntimeException e) {
            log.error(e, "Exception while authenticating request from operator");
            throw new ForbiddenException("Authentication error", e);
        }
    }
}
