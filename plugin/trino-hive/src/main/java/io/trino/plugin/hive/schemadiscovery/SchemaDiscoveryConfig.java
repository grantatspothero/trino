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
package io.trino.plugin.hive.schemadiscovery;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Min;

public class SchemaDiscoveryConfig
{
    private int executorThreadCount = 100;

    @Min(1)
    public int getExecutorThreadCount()
    {
        return executorThreadCount;
    }

    @Config("hive.schema-discovery.threads")
    @ConfigDescription("Number of threads for schema discovery executor")
    public SchemaDiscoveryConfig setExecutorThreadCount(int executorThreadCount)
    {
        this.executorThreadCount = executorThreadCount;
        return this;
    }
}
