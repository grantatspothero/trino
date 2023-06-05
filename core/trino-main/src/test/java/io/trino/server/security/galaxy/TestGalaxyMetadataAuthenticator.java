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

import io.airlift.security.pem.PemWriter;
import io.starburst.stargate.id.TrinoPlaneId;
import io.trino.server.metadataonly.MetadataOnlyConfig;
import org.testng.annotations.Test;

import java.security.KeyPair;
import java.util.Optional;

import static io.trino.server.security.galaxy.GalaxyIdentity.DISPATCH_IDENTITY_TYPE;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.DISPATCH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGalaxyMetadataAuthenticator
        extends TestGalaxyAuthenticator
{
    private static final String TRINO_PLANE_ID = "aws-us-fake1-1";
    private static final String DISPATCH_TOKEN_ISSUER = "https://issuer.dispatcher.example.com";

    @Test
    public void test()
            throws Exception
    {
        KeyPair keyPair = generateKeyPair();
        GalaxyMetadataAuthenticatorConfig authenticatorConfig = new GalaxyMetadataAuthenticatorConfig()
                .setPublicKey(PemWriter.writePublicKey(keyPair.getPublic()))
                .setDispatchTokenIssuer(DISPATCH_TOKEN_ISSUER)
                .setPortalTokenIssuer(TOKEN_ISSUER);
        MetadataOnlyConfig metadataOnlyConfig = new MetadataOnlyConfig()
                .setTrinoPlaneId(new TrinoPlaneId(TRINO_PLANE_ID));
        GalaxyMetadataAuthenticator authenticator = new GalaxyMetadataAuthenticator(authenticatorConfig, metadataOnlyConfig);
        test(TRINO_PLANE_ID, keyPair, authenticator::authenticate);
    }

    @Test
    public void testMultipleIssuers()
            throws Exception
    {
        KeyPair keyPair = generateKeyPair();
        GalaxyMetadataAuthenticatorConfig authenticatorConfig = new GalaxyMetadataAuthenticatorConfig()
                .setPublicKey(PemWriter.writePublicKey(keyPair.getPublic()))
                .setPortalTokenIssuer(TOKEN_ISSUER)
                .setDispatchTokenIssuer(DISPATCH_TOKEN_ISSUER);
        MetadataOnlyConfig metadataOnlyConfig = new MetadataOnlyConfig()
                .setTrinoPlaneId(new TrinoPlaneId(TRINO_PLANE_ID));
        GalaxyMetadataAuthenticator authenticator = new GalaxyMetadataAuthenticator(authenticatorConfig, metadataOnlyConfig);
        test(TRINO_PLANE_ID, keyPair, authenticator::authenticate, DISPATCH_TOKEN_ISSUER, DISPATCH_IDENTITY_TYPE);

        assertThat(authenticator.authenticate(generateJwt("username", ACCOUNT_ID, TRINO_PLANE_ID, keyPair.getPrivate(), notExpired(), requestNotExpired(), Optional.of("good"), DISPATCH_TOKEN_ISSUER, Optional.of(DISPATCH_IDENTITY_TYPE)), Optional.empty()))
                .satisfies(identity -> assertThat(identity.getUser()).isEqualTo("username"))
                .satisfies(identity -> assertThat(identity.getPrincipal().orElseThrow().toString()).isEqualTo(GALAXY_IDENTITY))
                .satisfies(identity -> assertThat(identity.getExtraCredentials().get("identityType")).isEqualTo(DISPATCH.name()));
    }
}
