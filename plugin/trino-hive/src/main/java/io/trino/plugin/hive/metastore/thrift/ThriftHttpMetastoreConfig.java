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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class ThriftHttpMetastoreConfig
{
    public enum AuthenticationMode
    {
        BEARER
    }

    private AuthenticationMode authenticationMode;
    private String httpBearerToken;
    private Map<String, String> additionalHeaders = ImmutableMap.of();

    @NotNull
    public Optional<AuthenticationMode> getAuthenticationMode()
    {
        return Optional.ofNullable(authenticationMode);
    }

    @Config("hive.metastore.http.client.authentication.type")
    @ConfigDescription("Authentication mode for thrift http based metastore client")
    public ThriftHttpMetastoreConfig setAuthenticationMode(AuthenticationMode mode)
    {
        this.authenticationMode = mode;
        return this;
    }

    public String getHttpBearerToken()
    {
        return httpBearerToken;
    }

    @Config("hive.metastore.http.client.bearer-token")
    @ConfigSecuritySensitive
    @ConfigDescription("Bearer token to authenticate with a HTTP transport based metastore service")
    public ThriftHttpMetastoreConfig setHttpBearerToken(String httpBearerToken)
    {
        this.httpBearerToken = httpBearerToken;
        return this;
    }

    public Map<String, String> getAdditionalHeaders()
    {
        return additionalHeaders;
    }

    @Config("hive.metastore.http.client.additional-headers")
    @ConfigDescription("Comma separated key:value pairs to be send to metastore as additional headers")
    public ThriftHttpMetastoreConfig setAdditionalHeaders(List<String> httpHeaders)
    {
        try {
            if (httpHeaders != null) {
                this.additionalHeaders = httpHeaders.stream()
                        .collect(toImmutableMap(kvs -> kvs.split(":", 2)[0], kvs -> kvs.split(":", 2)[1]));
            }
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format("Invalid format for 'hive.metastore.http.client.additional-headers'. " +
                    "Value provided is %s", Joiner.on(",").join(httpHeaders)), e);
        }
        return this;
    }
}
