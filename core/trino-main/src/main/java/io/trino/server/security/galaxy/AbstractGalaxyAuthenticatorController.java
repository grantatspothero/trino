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

import io.airlift.log.Logger;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.JwtParserBuilder;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.trino.server.security.AuthenticationException;

import java.security.PublicKey;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.jsonwebtoken.ClaimJwtException.INCORRECT_EXPECTED_CLAIM_MESSAGE_TEMPLATE;
import static io.jsonwebtoken.ClaimJwtException.MISSING_EXPECTED_CLAIM_MESSAGE_TEMPLATE;
import static io.jsonwebtoken.Claims.AUDIENCE;
import static io.jsonwebtoken.Claims.ISSUER;
import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.IdentityParams;
import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.RequestBodyHashing;
import static io.trino.server.security.galaxy.GalaxyIdentity.toGalaxyIdentityType;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

public abstract class AbstractGalaxyAuthenticatorController
{
    static final String REQUEST_EXPIRATION_CLAIM = "request_expiry";
    private static final Logger log = Logger.get(AbstractGalaxyAuthenticatorController.class);
    protected final JwtParser jwtParser;
    private final Map<String, Set<String>> issuerAudienceMapping;

    protected AbstractGalaxyAuthenticatorController(Map<String, Set<String>> issuerAudienceMapping, String subject, PublicKey publicKey)
    {
        this.issuerAudienceMapping = requireNonNull(issuerAudienceMapping, "issuerAudienceMapping is null");
        checkArgument(issuerAudienceMapping.size() > 0, "issuerAudienceMapping requires at least 1 issuer");
        JwtParserBuilder builder = newJwtParserBuilder()
                .setSigningKey(publicKey)
                .requireSubject(subject);
        this.jwtParser = builder.build();
    }

    protected IdentityParams commonAuthenticate(String token, Optional<RequestBodyHashing> requestBodyHashing)
            throws AuthenticationException
    {
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
        if (claims.getIssuer() == null) {
            String msg = String.format(MISSING_EXPECTED_CLAIM_MESSAGE_TEMPLATE,
                    ISSUER, issuerAudienceMapping.keySet());
            throw new JwtException(msg);
        }
        Set<String> audiences = issuerAudienceMapping.get(claims.getIssuer());
        if (audiences == null) {
            String msg = String.format(INCORRECT_EXPECTED_CLAIM_MESSAGE_TEMPLATE,
                    ISSUER, issuerAudienceMapping.keySet(), claims.getIssuer());
            throw new JwtException(msg);
        }
        else {
            if (!audiences.isEmpty()) {
                if (claims.getAudience() == null) {
                    String msg = String.format(MISSING_EXPECTED_CLAIM_MESSAGE_TEMPLATE,
                            AUDIENCE, audiences);
                    throw new JwtException(msg);
                }
                if (!audiences.contains(claims.getAudience())) {
                    String msg = String.format(INCORRECT_EXPECTED_CLAIM_MESSAGE_TEMPLATE,
                            AUDIENCE, audiences, claims.getAudience());
                    throw new JwtException(msg);
                }
            }
        }
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

        GalaxyIdentity.GalaxyIdentityType identityType = toGalaxyIdentityType(claims.get("identity_type", String.class));
        return new IdentityParams(username, accountId, userId, roleId, identityType, claims);
    }
}
