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
package io.trino.connector.informationschema.galaxy;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.http.client.HttpStatus;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.WebApplicationException;

import java.lang.invoke.MethodHandle;
import java.net.URI;
import java.util.OptionalLong;

import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.ERROR_RECENTLY_REFRESHED;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheEndpoint.ENDPOINT_REFRESH_CACHE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class GalaxyCacheRefreshCacheProcedure
        implements Provider<Procedure>
{
    private static final String REFRESH_PROCEDURE_NAME = "refresh";

    private static final MethodHandle REFRESH_CACHE = methodHandle(GalaxyCacheRefreshCacheProcedure.class, "refreshCache", String.class, ConnectorSession.class);

    private final GalaxyCacheClient galaxyCacheClient;

    @Inject
    public GalaxyCacheRefreshCacheProcedure(GalaxyCacheClient galaxyCacheClient)
    {
        this.galaxyCacheClient = requireNonNull(galaxyCacheClient, "galaxyCacheClient is null");
    }

    @UsedByGeneratedCode
    public void refreshCache(String catalog, ConnectorSession connectorSession)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalog);
        URI uri = galaxyCacheClient.uriBuilder(session, prefix, ENDPOINT_REFRESH_CACHE, OptionalLong.empty()).build();
        try {
            galaxyCacheClient.queryResults(session, prefix, ENDPOINT_REFRESH_CACHE, uri);
        }
        catch (NotFoundException e) {
            throw new TrinoException(GENERIC_USER_ERROR, e.getMessage(), e);
        }
        catch (WebApplicationException e) {
            if (e.getResponse().getStatus() == HttpStatus.CONFLICT.code()) {
                throw new NotFoundException(ERROR_RECENTLY_REFRESHED.apply(prefix.getCatalogName()));
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                GalaxyCacheConstants.SCHEMA_NAME,
                REFRESH_PROCEDURE_NAME,
                ImmutableList.<Procedure.Argument>builder()
                        .add(new Procedure.Argument("CATALOG", VARCHAR))
                        .build(),
                REFRESH_CACHE.bindTo(this));
    }
}
