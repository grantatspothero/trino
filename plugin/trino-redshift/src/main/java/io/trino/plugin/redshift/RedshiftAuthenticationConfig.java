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
package io.trino.plugin.redshift;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class RedshiftAuthenticationConfig
{
    private RedshiftAuthenticationType authenticationType = RedshiftAuthenticationType.PASSWORD;

    public enum RedshiftAuthenticationType
    {
        PASSWORD,
        AWS,
    }

    @NotNull
    public RedshiftAuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("redshift.authentication.type")
    @ConfigDescription("Redshift authentication mechanism")
    public RedshiftAuthenticationConfig setAuthenticationType(RedshiftAuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }
}
