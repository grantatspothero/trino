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

import com.google.inject.Inject;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunction;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionApi;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.galaxy.GalaxyIdentity.toDispatchSession;
import static java.util.Objects.requireNonNull;

public class GalaxyCatalogMetadata
        implements ConnectorMetadata
{
    private final GalaxyLanguageFunctionApi galaxyFunctionApi;

    @Inject
    public GalaxyCatalogMetadata(GalaxyLanguageFunctionApi galaxyFunctionApi)
    {
        this.galaxyFunctionApi = requireNonNull(galaxyFunctionApi, "galaxyFunctionApi is null");
    }

    @Override
    public Collection<LanguageFunction> listLanguageFunctions(ConnectorSession session, String schemaName)
    {
        DispatchSession dispatchSession = toDispatchSession(session.getIdentity());
        return galaxyFunctionApi.listLanguageFunctions(dispatchSession, schemaName).stream()
                .map(this::map)
                .collect(toImmutableSet());
    }

    @Override
    public Collection<LanguageFunction> getLanguageFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        DispatchSession dispatchSession = toDispatchSession(session.getIdentity());
        return galaxyFunctionApi.getLanguageFunctions(dispatchSession, map(name)).stream()
                .map(this::map)
                .collect(toImmutableSet());
    }

    @Override
    public boolean languageFunctionExists(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        DispatchSession dispatchSession = toDispatchSession(session.getIdentity());
        return galaxyFunctionApi.languageFunctionExists(dispatchSession, map(name), signatureToken);
    }

    @Override
    public void createLanguageFunction(ConnectorSession session, SchemaFunctionName name, LanguageFunction function, boolean replace)
    {
        DispatchSession dispatchSession = toDispatchSession(session.getIdentity());
        List<io.starburst.stargate.accesscontrol.client.CatalogSchemaName> paths = function.path()
                .stream()
                .map(catalogSchemaName -> new io.starburst.stargate.accesscontrol.client.CatalogSchemaName(catalogSchemaName.getCatalogName(), catalogSchemaName.getSchemaName()))
                .collect(toImmutableList());
        GalaxyLanguageFunction galaxyLanguageFunction = new GalaxyLanguageFunction(function.signatureToken(), function.sql(), paths, function.owner());
        galaxyFunctionApi.createLanguageFunction(dispatchSession, map(name), galaxyLanguageFunction, replace);
    }

    @Override
    public void dropLanguageFunction(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        DispatchSession dispatchSession = toDispatchSession(session.getIdentity());
        galaxyFunctionApi.dropLanguageFunction(dispatchSession, map(name), signatureToken);
    }

    private io.starburst.stargate.accesscontrol.client.SchemaFunctionName map(SchemaFunctionName name)
    {
        return new io.starburst.stargate.accesscontrol.client.SchemaFunctionName(name.getSchemaName(), name.getFunctionName());
    }

    private LanguageFunction map(GalaxyLanguageFunction galaxyLanguageFunction)
    {
        List<CatalogSchemaName> paths = galaxyLanguageFunction.path().stream()
                .map(name -> new CatalogSchemaName(name.catalogName(), name.schemaName()))
                .collect(toImmutableList());
        return new LanguageFunction(galaxyLanguageFunction.signatureToken(), galaxyLanguageFunction.sql(), paths, galaxyLanguageFunction.owner());
    }
}
