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
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.trino.server.security.AuthenticationException;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.util.Optional;

import static com.google.common.hash.Hashing.sha256;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.util.Objects.requireNonNull;

public final class GalaxyAuthenticationHelper
{
    private static final String GALAXY_AUTHENTICATION_HEADER = "X-Galaxy-Authentication";
    private static final Logger log = Logger.get(GalaxyAuthenticationHelper.class);

    private GalaxyAuthenticationHelper()
    {}

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

    public static Optional<String> extractToken(ContainerRequestContext request)
            throws AuthenticationException
    {
        String token = request.getHeaderString(GALAXY_AUTHENTICATION_HEADER);
        if (token != null) {
            request.getHeaders().remove(GALAXY_AUTHENTICATION_HEADER);
            return Optional.of(token);
        }
        log.error("No Galaxy token present");
        return Optional.empty();
    }

    static String hashBody(ContainerRequestContext request)
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

    record IdentityParams(String username, AccountId accountId, UserId userId, RoleId roleId, GalaxyIdentityType identityType, Claims claims) {}

    record RequestBodyHashing(String hash, String claimName)
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
}
