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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.NotEmpty;

import java.io.File;
import java.util.Map;
import java.util.Set;

import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.DISPATCH;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.PORTAL;

public class GalaxyMetadataAuthenticatorConfig
{
    private String portalTokenIssuer;
    private String dispatchTokenIssuer;
    Map<String, Set<String>> issuersAndAudiences = ImmutableMap.of();
    Set<GalaxyIdentityType> identityTypes = ImmutableSet.of();

    private String publicKey;
    private File publicKeyFile;

    @VisibleForTesting
    public Map<String, Set<String>> getTokenIssuers()
    {
        return issuersAndAudiences;
    }

    @VisibleForTesting
    public Set<GalaxyIdentityType> getTokenIdenityTypes()
    {
        return identityTypes;
    }

    @NotEmpty
    public String getTokenIssuer()
    {
        return portalTokenIssuer;
    }

    @Config("galaxy.authentication.token-issuer")
    public GalaxyMetadataAuthenticatorConfig setTokenIssuer(String tokenIssuer)
    {
        this.portalTokenIssuer = tokenIssuer;
        updateImmutables();
        return this;
    }

    @NotEmpty
    public String getDispatchTokenIssuer()
    {
        return dispatchTokenIssuer;
    }

    @Config("galaxy.authentication.dispatch-token-issuer")
    public GalaxyMetadataAuthenticatorConfig setDispatchTokenIssuer(String tokenIssuer)
    {
        this.dispatchTokenIssuer = tokenIssuer;
        updateImmutables();
        return this;
    }

    private void updateImmutables()
    {
        ImmutableMap.Builder<String, Set<String>> issuerBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<GalaxyIdentityType> identityTypeBuilder = ImmutableSet.builder();
        if (portalTokenIssuer != null) {
            issuerBuilder.put(portalTokenIssuer, ImmutableSet.of());
            identityTypeBuilder.add(PORTAL);
        }
        if (dispatchTokenIssuer != null) {
            issuerBuilder.put(dispatchTokenIssuer, ImmutableSet.of());
            identityTypeBuilder.add(DISPATCH);
        }
        issuersAndAudiences = issuerBuilder.buildOrThrow();
        identityTypes = identityTypeBuilder.build();
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
