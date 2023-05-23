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
import io.jsonwebtoken.JwtException;
import io.trino.server.security.AuthenticationException;
import io.trino.spi.security.Identity;

import java.security.PublicKey;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.jsonwebtoken.ClaimJwtException.INCORRECT_EXPECTED_CLAIM_MESSAGE_TEMPLATE;
import static io.jsonwebtoken.ClaimJwtException.MISSING_EXPECTED_CLAIM_MESSAGE_TEMPLATE;
import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.IdentityParams;
import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.RequestBodyHashing;
import static io.trino.server.security.galaxy.GalaxyAuthenticationHelper.parseClaimsWithoutValidation;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType;
import static io.trino.server.security.galaxy.GalaxyIdentity.createIdentity;

/**
 * This class is a specialization of {@link AbstractGalaxyAuthenticatorController} that allows a resource
 * to be accessed from multiple issuers.
 * <p>
 * Whereas some resources only check issuer, resources that use this class expect both the issuer and
 * the identityType to be present in the claims.
 */
public final class GalaxyMetadataAuthenticatorController
        extends AbstractGalaxyAuthenticatorController
{
    private final Set<GalaxyIdentityType> metadataIdentityTypes;
    private static final Logger log = Logger.get(GalaxyMetadataAuthenticatorController.class);
    private static final String IDENTITY_TYPE = "identity_type";

    public GalaxyMetadataAuthenticatorController(Map<String, Set<String>> issuerAudienceMapping, Set<GalaxyIdentityType> identityTypes, String subject, PublicKey publicKey)
    {
        super(issuerAudienceMapping, subject, publicKey);
        metadataIdentityTypes = identityTypes;
    }

    public Identity authenticate(String token, Optional<RequestBodyHashing> requestBodyHashing)
            throws AuthenticationException
    {
        try {
            IdentityParams identityParams = commonAuthenticate(token, requestBodyHashing);
            // Need to perform check against the identity type
            if (identityParams.identityType() == null) {
                String msg = String.format(MISSING_EXPECTED_CLAIM_MESSAGE_TEMPLATE,
                        IDENTITY_TYPE, metadataIdentityTypes);
                throw new JwtException(msg);
            }
            else {
                if (!metadataIdentityTypes.contains(identityParams.identityType())) {
                    String msg = String.format(INCORRECT_EXPECTED_CLAIM_MESSAGE_TEMPLATE,
                            IDENTITY_TYPE, metadataIdentityTypes, identityParams.identityType());
                    throw new JwtException(msg);
                }
            }
            return createIdentity(identityParams.username(), identityParams.accountId(), identityParams.userId(), identityParams.roleId(), token, identityParams.identityType());
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
}
