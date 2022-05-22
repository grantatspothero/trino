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

public class TestGalaxyMetadataAuthenticator
        extends TestGalaxyAuthenticator
{
    private static final String TRINO_PLANE_ID = "aws-us-fake1-1";

    @Test
    public void test()
            throws Exception
    {
        KeyPair keyPair = generateKeyPair();
        GalaxyMetadataAuthenticatorConfig authenticatorConfig = new GalaxyMetadataAuthenticatorConfig()
                .setPublicKey(PemWriter.writePublicKey(keyPair.getPublic()))
                .setTokenIssuer(TOKEN_ISSUER);
        MetadataOnlyConfig metadataOnlyConfig = new MetadataOnlyConfig()
                .setTrinoPlaneId(new TrinoPlaneId(TRINO_PLANE_ID));
        GalaxyMetadataAuthenticator authenticator = new GalaxyMetadataAuthenticator(authenticatorConfig, metadataOnlyConfig);
        test(TRINO_PLANE_ID, keyPair, authenticator::authenticate);
    }
}
