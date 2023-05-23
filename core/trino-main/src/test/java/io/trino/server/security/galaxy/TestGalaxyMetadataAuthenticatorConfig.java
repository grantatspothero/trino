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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.DISPATCH;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.PORTAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestGalaxyMetadataAuthenticatorConfig
{
    private static final String PORTAL_TOKEN_ISSUER = "https://issuer.example.com";
    private static final String DISPATCH_TOKEN_ISSUER = "https://issuer.dispatcher.example.com";

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GalaxyMetadataAuthenticatorConfig.class)
                .setTokenIssuer(null)
                .setDispatchTokenIssuer(null)
                .setPublicKey(null)
                .setPublicKeyFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path publicKeyFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("galaxy.authentication.token-issuer", PORTAL_TOKEN_ISSUER)
                .put("galaxy.authentication.dispatch-token-issuer", DISPATCH_TOKEN_ISSUER)
                .put("galaxy.authentication.public-key", "some key")
                .put("galaxy.authentication.public-key-file", publicKeyFile.toString())
                .buildOrThrow();

        GalaxyMetadataAuthenticatorConfig expected = new GalaxyMetadataAuthenticatorConfig()
                .setTokenIssuer(PORTAL_TOKEN_ISSUER)
                .setDispatchTokenIssuer(DISPATCH_TOKEN_ISSUER)
                .setPublicKey("some key")
                .setPublicKeyFile(publicKeyFile.toFile());

        assertFullMapping(properties, expected);

        Files.delete(publicKeyFile);
    }

    @Test
    public void testIssuerMapCleanup()
            throws IOException
    {
        Map<String, Set<String>> expectedIssuers = ImmutableMap.<String, Set<String>>builder()
                .put(PORTAL_TOKEN_ISSUER, ImmutableSet.of())
                .put(DISPATCH_TOKEN_ISSUER, ImmutableSet.of())
                .buildOrThrow();
        Set<GalaxyIdentityType> expectedIdentityTypes = ImmutableSet.<GalaxyIdentityType>builder()
                .add(PORTAL)
                .add(DISPATCH)
                .build();

        GalaxyMetadataAuthenticatorConfig config = new GalaxyMetadataAuthenticatorConfig()
                .setTokenIssuer(PORTAL_TOKEN_ISSUER)
                .setDispatchTokenIssuer(DISPATCH_TOKEN_ISSUER);
        assertEquals(expectedIssuers, config.getTokenIssuers());
        assertEquals(expectedIdentityTypes, config.getTokenIdenityTypes());

        config.setTokenIssuer(null);
        assertEquals(1, config.getTokenIssuers().size());
        assertEquals(1, config.getTokenIdenityTypes().size());
        assertEquals(config.getTokenIssuers().keySet().iterator().next(), DISPATCH_TOKEN_ISSUER);
        assertEquals(config.getTokenIdenityTypes().iterator().next(), DISPATCH);

        config.setDispatchTokenIssuer(null);
        assertTrue(config.getTokenIssuers().isEmpty());
        assertTrue(config.getTokenIdenityTypes().isEmpty());
    }
}
