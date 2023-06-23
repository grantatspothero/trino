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
package io.trino.server.galaxy;

import io.airlift.log.Logger;

import javax.ws.rs.core.HttpHeaders;

import java.util.Optional;
import java.util.function.UnaryOperator;

import static com.google.common.base.Strings.isNullOrEmpty;

public final class GalaxyBearerToken
{
    private static final Logger log = Logger.get(GalaxyBearerToken.class);

    private GalaxyBearerToken() {}

    public static Optional<String> extractToken(UnaryOperator<String> headerSupplier, String context)
    {
        String header = headerSupplier.apply(HttpHeaders.AUTHORIZATION);
        if (isNullOrEmpty(header) || header.isBlank()) {
            log.error("No headers present in the request. Context: %s".formatted(context));
            return Optional.empty();
        }

        int space = header.indexOf(' ');
        if ((space < 0) || !header.substring(0, space).equalsIgnoreCase("bearer")) {
            log.error("Malformed credentials: token missing or wrong token format. Context: %s".formatted(context));
            return Optional.empty();
        }
        String token = header.substring(space + 1).trim();
        return Optional.of(token);
    }
}
