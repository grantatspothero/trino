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

import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.trino.client.Column;
import io.trino.spi.QueryId;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class ResultsCacheClient
{
    private final HttpClient httpClient;

    public ResultsCacheClient(HttpClient httpClient)
    {
        this.httpClient = httpClient;

        // Temporary usage of httpClient to pass tests
        checkState(this.httpClient != null, "httpClient is null");
    }

    public void uploadResultsCacheEntry(
            String key,
            QueryId queryId,
            List<String> catalogs,
            List<String> schemas,
            List<String> tables,
            List<Column> columns,
            List<List<Object>> data,
            Duration expirationInterval)
    {
        // TODO https://github.com/starburstdata/stargate/issues/8383
    }
}
