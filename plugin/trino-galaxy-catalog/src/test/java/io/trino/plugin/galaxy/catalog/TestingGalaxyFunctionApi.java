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
package io.trino.plugin.galaxy.catalog;

import com.google.common.collect.ImmutableList;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunction;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionApi;
import io.starburst.stargate.accesscontrol.client.SchemaFunctionName;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.spi.TrinoException;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;

public class TestingGalaxyFunctionApi
        implements GalaxyLanguageFunctionApi
{
    private final Map<SchemaFunctionName, GalaxyLanguageFunction> functions = new ConcurrentHashMap<>();

    @Override
    public Collection<GalaxyLanguageFunction> listLanguageFunctions(DispatchSession session, String schemaName)
    {
        return functions.values();
    }

    @Override
    public Collection<GalaxyLanguageFunction> getLanguageFunctions(DispatchSession session, SchemaFunctionName name)
    {
        GalaxyLanguageFunction function = functions.get(name);
        return (function != null) ? ImmutableList.of(function) : ImmutableList.of();
    }

    @Override
    public boolean languageFunctionExists(DispatchSession session, SchemaFunctionName name, String signatureToken)
    {
        GalaxyLanguageFunction function = functions.get(name);
        return (function != null) && function.signatureToken().equals(signatureToken);
    }

    @Override
    public Optional<GalaxyLanguageFunction> getFunctionBySignatureToken(DispatchSession session, SchemaFunctionName name, String signatureToken)
    {
        GalaxyLanguageFunction function = functions.get(name);
        if (function != null && function.signatureToken().equals(signatureToken)) {
            return Optional.of(function);
        }
        return Optional.empty();
    }

    @Override
    public void createLanguageFunction(DispatchSession session, SchemaFunctionName name, GalaxyLanguageFunction function, boolean replace)
    {
        if (replace) {
            functions.put(name, function);
        }
        else {
            if (functions.putIfAbsent(name, function) != null) {
                throw new TrinoException(ALREADY_EXISTS, "Function already exists");
            }
        }
    }

    @Override
    public void dropLanguageFunction(DispatchSession session, SchemaFunctionName name, String signatureToken)
    {
        if (functions.remove(name) == null) {
            throw new TrinoException(NOT_FOUND, "Function does not exists");
        }
    }
}
