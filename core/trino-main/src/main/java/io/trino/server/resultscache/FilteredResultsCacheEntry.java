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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FilteredResultsCacheEntry
        implements ResultsCacheEntry
{
    private final Optional<ResultsCacheResult> filteredResult;

    public FilteredResultsCacheEntry(ResultsCacheResult.Status filteredStatus)
    {
        checkArgument(
                filteredStatus.isFiltered(),
                "filteredStatus %s is not a filtered status".formatted(filteredStatus));
        this.filteredResult =
                Optional.of(new ResultsCacheResult(requireNonNull(filteredStatus, "filteredResult is null")));
    }

    @Override
    public void done()
    {
    }

    @Override
    public boolean isDone()
    {
        return true;
    }

    @Override
    public boolean addTransitionToDoneCallback(DoneCallback doneCallback)
    {
        // Does not transition, doneCallback will never be called
        return false;
    }

    @Override
    public Optional<ResultsCacheResult> getEntryResult()
    {
        return filteredResult;
    }
}
