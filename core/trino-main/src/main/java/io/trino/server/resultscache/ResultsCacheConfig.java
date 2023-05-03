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

package io.trino.server.resultscache;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

public class ResultsCacheConfig
{
    private int cacheUploadThreads = 5;
    private String cacheEndpoint = "http://results-cache.trino-results-cache.svc:8080";

    @Positive
    public int getCacheUploadThreads()
    {
        return cacheUploadThreads;
    }

    @Config("galaxy.results-cache.upload.threads")
    @ConfigDescription("Size of thread pool for uploading ResultSets to the ResultSet Cache")
    public ResultsCacheConfig setCacheUploadThreads(int cacheUploadThreads)
    {
        this.cacheUploadThreads = cacheUploadThreads;
        return this;
    }

    @NotNull
    public String getCacheEndpoint()
    {
        return cacheEndpoint;
    }

    @Config("galaxy-results-cache.upload.endpoint")
    @ConfigDescription("Endpoint of local ResultSet Cache Instance")
    public ResultsCacheConfig setCacheEndpoint(String cacheEndpoint)
    {
        this.cacheEndpoint = cacheEndpoint;
        return this;
    }
}
