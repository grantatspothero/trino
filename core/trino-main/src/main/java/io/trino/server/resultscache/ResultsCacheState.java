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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ResultsCacheState
{
    private final String key;
    private final Optional<Long> maximumSizeBytes;

    public ResultsCacheState(String key, Optional<Long> maximumSizeBytes)
    {
        this.key = requireNonNull(key, "key is null");
        this.maximumSizeBytes = requireNonNull(maximumSizeBytes, "maximumSizeBytes is null");
    }

    public String key()
    {
        return key;
    }

    public Optional<Long> maximumSizeBytes()
    {
        return maximumSizeBytes;
    }
}
