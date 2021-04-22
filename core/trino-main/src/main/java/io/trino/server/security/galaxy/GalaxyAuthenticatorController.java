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

import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;
import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.JwtParserBuilder;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.trino.server.security.AuthenticationException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static com.google.common.hash.Hashing.sha256;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.lang.String.format;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

public class GalaxyAuthenticatorController
{
    private static final Logger log = Logger.get(GalaxyAuthenticatorController.class);

    static final String REQUEST_EXPIRATION_CLAIM = "request_expiry";
    private static final String GALAXY_TOKEN_CREDENTIAL = "GalaxyTokenCredential";

    private final JwtParser jwtParser;

    private static final String GALAXY_AUTHENTICATION_HEADER = "X-Galaxy-Authentication";

    static PublicKey loadPublicKey(Optional<String> publicKey, Optional<File> publicKeyFile)
            throws GeneralSecurityException, IOException
    {
        if (publicKey.isPresent()) {
            return PemReader.loadPublicKey(publicKey.get());
        }
        if (publicKeyFile.isPresent()) {
            return PemReader.loadPublicKey(publicKeyFile.get());
        }
        throw new IllegalArgumentException("Both publicKey and publicKeyFile are empty");
    }

    GalaxyAuthenticatorController(String issuer, Optional<String> audience, String subject, PublicKey publicKey)
    {
        JwtParserBuilder builder = newJwtParserBuilder()
                .setSigningKey(publicKey)
                .requireIssuer(issuer)
                .requireSubject(subject);
        audience.ifPresent(builder::requireAudience);
        this.jwtParser = builder.build();
    }

    public static Optional<String> parseClaimsWithoutValidation(String jws)
    {
        try {
            String value = jws.substring(0, jws.lastIndexOf('.') + 1);
            return Optional.of(newJwtParserBuilder().build().parse(value).getBody().toString());
        }
        catch (RuntimeException ignored) {
            return Optional.empty();
        }
    }

    public record RequestBodyHashing(String hash, String claimName)
    {
        public RequestBodyHashing
        {
            requireNonNull(hash, "hash is null");
            requireNonNull(claimName, "claimName is null");
        }

        public RequestBodyHashing(ContainerRequestContext request, String claimName)
        {
            this(hashBody(request), claimName);
        }
    }

    public Identity authenticate(String token, Optional<RequestBodyHashing> requestBodyHashing)
            throws AuthenticationException
    {
        try {
            Claims claims = jwtParser.parseClaimsJws(token).getBody();
            String username = claims.get("username", String.class);
            if (username == null) {
                throw new AuthenticationException("Invalid username", "Galaxy");
            }
            AccountId accountId;
            try {
                accountId = new AccountId(claims.getAudience());
            }
            catch (IllegalArgumentException e) {
                throw new AuthenticationException("Invalid audience", "Galaxy");
            }
            UserId userId = new UserId(requireNonNull(claims.get("user_id", String.class), "userId is null"));
            RoleId roleId = new RoleId(requireNonNull(claims.get("role_id", String.class), "roleId is null"));

            if (requestBodyHashing.isPresent()) {
                if (!claims.containsKey(REQUEST_EXPIRATION_CLAIM) || now().isAfter(claims.get(REQUEST_EXPIRATION_CLAIM, Date.class).toInstant())) {
                    String expiration = Optional.ofNullable(claims.get(REQUEST_EXPIRATION_CLAIM, Date.class)).map(Date::toInstant).map(Instant::toString).orElse("null");
                    log.error("Attempt to use expired (as of %s) token for user: %s %s %s", expiration, accountId, userId, roleId);
                    throw new AuthenticationException("Token expired", "Galaxy");
                }

                String requestHash = requestBodyHashing.get().hash();
                if (!requestHash.equals(claims.get(requestBodyHashing.get().claimName()))) {
                    log.error("Request body hash does not match for user: %s %s %s", accountId, userId, roleId);
                    throw new AuthenticationException("Request body hash does not match", "Galaxy");
                }
            }

            return Identity.forUser(username)
                    .withPrincipal(new BasicPrincipal(format("galaxy:%s:%s", userId, roleId)))
                    .withExtraCredentials(Map.of(GALAXY_TOKEN_CREDENTIAL, token))
                    .build();
        }
        catch (JwtException e) {
            log.error(e, "Exception parsing JWT token: %s", parseClaimsWithoutValidation(token).orElse("not a JWT token"));
            throw new AuthenticationException(e.getMessage(), "Galaxy");
        }
        catch (RuntimeException e) {
            log.error(e, "Authentication error: %s", parseClaimsWithoutValidation(token).orElse("not a JWT token"));
            throw new RuntimeException("Authentication error", e);
        }
    }

    public static String extractToken(ContainerRequestContext request)
            throws AuthenticationException
    {
        String token = request.getHeaderString(GALAXY_AUTHENTICATION_HEADER);
        if (token != null) {
            request.getHeaders().remove(GALAXY_AUTHENTICATION_HEADER);
            return token;
        }
        log.error("No Galaxy token present");
        throw new AuthenticationException("Galaxy token is required", "Galaxy");
    }

    private static String hashBody(ContainerRequestContext request)
    {
        if ((request.getMediaType() == null) ||
                !"utf-8".equalsIgnoreCase(request.getMediaType().getParameters().get(MediaType.CHARSET_PARAMETER))) {
            log.error("Attempt to hash non UTF-8 request body with media type: %s", request.getMediaType());
            throw new BadRequestException("Request is not UTF-8");
        }
        try {
            byte[] body = ByteStreams.toByteArray(request.getEntityStream());
            String requestBodyHash = sha256().hashBytes(body).toString();
            request.setEntityStream(new ByteArrayInputStream(body));
            return requestBodyHash;
        }
        catch (IOException e) {
            log.error(e, "Error reading request body");
            throw new BadRequestException("Could not read request body", e);
        }
    }
}
