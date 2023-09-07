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

import java.net.URLEncoder;
import java.util.Optional;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class S3PathUtils
{
    private static final Pattern SLASHES = Pattern.compile("//+");

    private S3PathUtils() {}

    public static PathKeys keysFromPath(String key, boolean supportLegacyCorruptedPaths)
    {
        if (supportLegacyCorruptedPaths) {
            Optional<String> legacyCorruptedKey = legacyCorruptedKey(key);
            return new PathKeys(key, legacyCorruptedKey);
        }
        return new PathKeys(key, Optional.empty());
    }

    static Optional<String> legacyCorruptedKey(String correctKey)
    {
        String normalized = SLASHES.matcher(correctKey).replaceAll("/");
        if (normalized.startsWith("/")) {
            normalized = normalized.substring("/".length());
        }
        if (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - "/".length());
        }
        if (normalized.equals(correctKey)) {
            return Optional.empty();
        }

        String encoded = normalized + "#" + URLEncoder.encode("/" + correctKey, UTF_8);

        return Optional.of(encoded);
    }
}
