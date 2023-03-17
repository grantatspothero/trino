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

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.trino.spi.QueryId;

import java.util.List;

public class ResultsCacheManager
{
    private static final long DEFAULT_MAXIMUM_SIZE_BYTES = 1_048_576;

    private final ResultsCacheClient resultsCacheClient;

    @Inject
    public ResultsCacheManager(@ForResultsCache HttpClient httpClient)
    {
        this.resultsCacheClient = new ResultsCacheClient(httpClient);
    }

    public ResultsCacheEntry createResultsCacheEntry(ResultsCacheParameters resultsCacheParameters, QueryId queryId, List<String> catalogs, List<String> schemas, List<String> tables)
    {
        long maximumSizeBytes = resultsCacheParameters.maximumSizeBytes().orElse(DEFAULT_MAXIMUM_SIZE_BYTES);
        return new ResultsCacheEntry(
                resultsCacheParameters.key(),
                queryId,
                catalogs,
                schemas,
                tables,
                maximumSizeBytes,
                resultsCacheParameters.expirationInterval(),
                resultsCacheClient);
    }
}
