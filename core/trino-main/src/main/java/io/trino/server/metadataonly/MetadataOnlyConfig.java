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
package io.trino.server.metadataonly;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.starburst.stargate.id.TrinoPlaneId;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MetadataOnlyConfig
{
    private TrinoPlaneId trinoPlaneId;
    private boolean useKmsCrypto;
    private Duration connectorCacheDuration = new Duration(5, MINUTES);
    private Duration shutdownExitCheckDelay = new Duration(10, SECONDS);
    private Optional<String> shutdownAuthenticationKey = Optional.empty();

    @NotNull
    public TrinoPlaneId getTrinoPlaneId()
    {
        return trinoPlaneId;
    }

    @Config("trino.plane-id")
    public MetadataOnlyConfig setTrinoPlaneId(TrinoPlaneId trinoPlaneId)
    {
        this.trinoPlaneId = trinoPlaneId;
        return this;
    }

    public boolean getUseKmsCrypto()
    {
        return useKmsCrypto;
    }

    @Config("kms-crypto.enabled")
    public MetadataOnlyConfig setUseKmsCrypto(boolean useKmsCrypto)
    {
        this.useKmsCrypto = useKmsCrypto;
        return this;
    }

    @MinDuration("0s")
    public Duration getConnectorCacheDuration()
    {
        return connectorCacheDuration;
    }

    @Config("metadata.connector-cache.duration")
    public MetadataOnlyConfig setConnectorCacheDuration(Duration connectorCacheDuration)
    {
        this.connectorCacheDuration = connectorCacheDuration;
        return this;
    }

    @MinDuration("0s")
    public Duration getShutdownExitCheckDelay()
    {
        return shutdownExitCheckDelay;
    }

    @Config("metadata.shutdown.exit-delay")
    public MetadataOnlyConfig setShutdownExitCheckDelay(Duration shutdownExitCheckDelay)
    {
        this.shutdownExitCheckDelay = shutdownExitCheckDelay;
        return this;
    }

    @NotNull
    public Optional<String> getShutdownAuthenticationKey()
    {
        return shutdownAuthenticationKey;
    }

    @Config("metadata.shutdown.authentication-key")
    public MetadataOnlyConfig setShutdownAuthenticationKey(String shutdownAuthenticationKey)
    {
        this.shutdownAuthenticationKey = Optional.of(shutdownAuthenticationKey);
        return this;
    }
}
