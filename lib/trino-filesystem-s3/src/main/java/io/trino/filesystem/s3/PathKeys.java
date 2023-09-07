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
package io.trino.filesystem.s3;

import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public record PathKeys(String correctKey, Optional<String> legacyCorruptedKey)
{
    public PathKeys
    {
        requireNonNull(correctKey, "correctKey is null");
        requireNonNull(legacyCorruptedKey, "legacyCorruptedKey is null");
    }

    Stream<String> stream()
    {
        if (legacyCorruptedKey.isEmpty()) {
            return Stream.of(correctKey);
        }
        return Stream.of(correctKey, legacyCorruptedKey.get());
    }
}
