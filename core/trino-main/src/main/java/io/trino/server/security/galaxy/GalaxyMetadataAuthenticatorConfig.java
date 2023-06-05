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

import io.airlift.configuration.Config;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.NotEmpty;

import java.io.File;

import static java.util.Objects.requireNonNull;

public class GalaxyMetadataAuthenticatorConfig
{
    private String portalTokenIssuer;
    private String dispatchTokenIssuer;

    private String publicKey;
    private File publicKeyFile;

    @NotEmpty
    public String getPortalTokenIssuer()
    {
        return portalTokenIssuer;
    }

    @Config("galaxy.authentication.token-issuer")
    public GalaxyMetadataAuthenticatorConfig setPortalTokenIssuer(String portalTokenIssuer)
    {
        this.portalTokenIssuer = requireNonNull(portalTokenIssuer, "portalTokenIssuer is null");
        return this;
    }

    @NotEmpty
    public String getDispatchTokenIssuer()
    {
        return dispatchTokenIssuer;
    }

    @Config("galaxy.authentication.dispatch-token-issuer")
    public GalaxyMetadataAuthenticatorConfig setDispatchTokenIssuer(String dispatchTokenIssuer)
    {
        this.dispatchTokenIssuer = requireNonNull(dispatchTokenIssuer, "dispatchTokenIssuer is null");
        return this;
    }

    public String getPublicKey()
    {
        return publicKey;
    }

    @Config("galaxy.authentication.public-key")
    public GalaxyMetadataAuthenticatorConfig setPublicKey(String publicKey)
    {
        this.publicKey = publicKey;
        return this;
    }

    @FileExists
    public File getPublicKeyFile()
    {
        return publicKeyFile;
    }

    @Config("galaxy.authentication.public-key-file")
    public GalaxyMetadataAuthenticatorConfig setPublicKeyFile(File publicKeyFile)
    {
        this.publicKeyFile = publicKeyFile;
        return this;
    }
}
