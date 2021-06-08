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
package io.trino.server.galaxy;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

public class GalaxyOperatorAuthenticationConfig
{
    private String operatorTokenIssuer;
    private String operatorSharedSecret;

    public String getOperatorTokenIssuer()
    {
        return operatorTokenIssuer;
    }

    @Config("galaxy.operator.token-issuer")
    public GalaxyOperatorAuthenticationConfig setOperatorTokenIssuer(String operatorTokenIssuer)
    {
        this.operatorTokenIssuer = operatorTokenIssuer;
        return this;
    }

    public String getOperatorSharedSecret()
    {
        return operatorSharedSecret;
    }

    @Config("galaxy.operator.shared-secret")
    @ConfigDescription("Secret shared between the Galaxy Trino Operator and Trino coordinators for the purpose of authentication")
    @ConfigSecuritySensitive
    public GalaxyOperatorAuthenticationConfig setOperatorSharedSecret(String operatorSharedSecret)
    {
        this.operatorSharedSecret = operatorSharedSecret;
        return this;
    }
}
