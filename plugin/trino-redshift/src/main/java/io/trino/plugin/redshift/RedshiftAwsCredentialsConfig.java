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
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class RedshiftAwsCredentialsConfig
{
    private String connectionUser;
    private String regionName;
    private String accessKey;
    private String secretKey;

    @NotNull
    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("connection-user")
    @ConfigDescription("The AWS Redshift username to log in as")
    public RedshiftAwsCredentialsConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @NotNull
    public String getRegionName()
    {
        return regionName;
    }

    @Config("aws.region-name")
    public RedshiftAwsCredentialsConfig setRegionName(String regionName)
    {
        this.regionName = regionName;
        return this;
    }

    @NotNull
    public String getAccessKey()
    {
        return accessKey;
    }

    @Config("aws.access-key")
    public RedshiftAwsCredentialsConfig setAccessKey(@Nullable String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    @NotNull
    public String getSecretKey()
    {
        return secretKey;
    }

    @Config("aws.secret-key")
    @ConfigSecuritySensitive
    public RedshiftAwsCredentialsConfig setSecretKey(@Nullable String secretKey)
    {
        this.secretKey = secretKey;
        return this;
    }
}
