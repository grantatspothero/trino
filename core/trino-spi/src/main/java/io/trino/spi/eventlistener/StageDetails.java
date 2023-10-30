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
package io.trino.spi.eventlistener;

import static java.util.Objects.requireNonNull;

// TODO: remove time fields without unit after this has been in production for 30 days
public record StageDetails(
        String id,
        String state,
        long tasks,
        boolean fullyBlocked,
        long scheduledTime,
        long scheduledTimeMs,
        long blockedTime,
        long blockedTimeMs,
        long cpuTime,
        long cpuTimeMs,
        long inputBuffer)
{
    public StageDetails
    {
        requireNonNull(id, "id is null");
        requireNonNull(state, "state is null");
    }
}
