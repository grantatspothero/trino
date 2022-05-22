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
import io.starburst.stargate.id.TrinoPlaneId;

import javax.validation.constraints.NotNull;

public class MetadataOnlyConfig
{
    private TrinoPlaneId trinoPlaneId;
    private boolean useKmsCrypto;

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
}
