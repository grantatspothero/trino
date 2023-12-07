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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;

public class TestGalaxyMetadataAuthenticatorConfig
{
    private static final String PORTAL_TOKEN_ISSUER = "https://issuer.example.com";
    private static final String DISPATCH_TOKEN_ISSUER = "https://issuer.dispatcher.example.com";

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
                .setPortalTokenIssuer(PORTAL_TOKEN_ISSUER)
                .setDispatchTokenIssuer(DISPATCH_TOKEN_ISSUER)
                .setPublicKey("some key")
                .setPublicKeyFile(publicKeyFile.toFile());

        assertFullMapping(properties, expected);

        Files.delete(publicKeyFile);
    }
}
