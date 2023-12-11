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
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public interface ResultsCacheEntry
{
    void done();

    boolean isDone();

    /**
     * @return True, if `isDone` == false and the callback was added.  False, if the `isDone` == true.
     */
    boolean addTransitionToDoneCallback(DoneCallback doneCallback);

    Optional<ResultsCacheResult> getEntryResult();

    record ResultsCacheResult(Status status, long resultSetSize)
    {
        public enum Status
        {
            CACHING("caching", false),
            CACHED("cached", false),
            OVER_MAX_SIZE("over max size", true),
            NO_COLUMNS("no columns in result set", true),
            EXECUTE_STATEMENT("execute statement not supported", true),
            NOT_SELECT("not a select statement", true),
            QUERY_HAS_SYSTEM_TABLE("query has system table", true);

            private final String display;
            private final boolean isFiltered;

            Status(String display, boolean isFiltered)
            {
                requireNonNull(display, "display is null");
                this.display = display;
                this.isFiltered = isFiltered;
            }

            public String getDisplay()
            {
                return display;
            }

            public boolean isFiltered()
            {
                return isFiltered;
            }
        }

        public ResultsCacheResult(Status status)
        {
            this(status, 0);
        }

        public ResultsCacheResult(Status status, long resultSetSize)
        {
            this.status = requireNonNull(status, "status is null");
            checkArgument(resultSetSize >= 0, "resultSetSize cannot be negative");
            this.resultSetSize = resultSetSize;
        }

        public ResultsCacheResult withCurrentSize(long currentSize)
        {
            checkState(status == Status.CACHING, "can only update size in CACHEING state");
            checkArgument(currentSize >= resultSetSize, "new size is less than current size");
            return new ResultsCacheResult(Status.CACHING, currentSize);
        }
    }

    interface DoneCallback
    {
        void done();
    }
}
