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

import io.trino.Session;
import io.trino.connector.informationschema.InformationSchemaPageSource;
import io.trino.connector.informationschema.InformationSchemaTableHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import jakarta.ws.rs.NotFoundException;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static io.trino.connector.informationschema.galaxy.GalaxyCacheEndpoint.ENDPOINT_COLUMNS;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheEndpoint.ENDPOINT_SCHEMAS;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheEndpoint.ENDPOINT_TABLES;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheEndpoint.ENDPOINT_VIEWS;
import static java.util.Objects.requireNonNull;

public class GalaxyCacheInformationSchemaPageSource
        extends InformationSchemaPageSource
{
    private final GalaxyCacheClient galaxyCacheClient;
    private final Session session;

    public GalaxyCacheInformationSchemaPageSource(
            GalaxyCacheClient galaxyCacheClient,
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            InformationSchemaTableHandle tableHandle,
            List<ColumnHandle> columns)
    {
        super(session, metadata, accessControl, tableHandle, columns);

        this.galaxyCacheClient = requireNonNull(galaxyCacheClient, "galaxyCacheClient is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    protected void addColumnsRecords(QualifiedTablePrefix prefix)
    {
        doQuery(ENDPOINT_COLUMNS, prefix, super::addColumnsRecords);
    }

    @Override
    protected void addTablesRecords(QualifiedTablePrefix prefix)
    {
        doQuery(ENDPOINT_TABLES, prefix, super::addTablesRecords);
    }

    @Override
    protected void addViewsRecords(QualifiedTablePrefix prefix)
    {
        doQuery(ENDPOINT_VIEWS, prefix, super::addViewsRecords);
    }

    @Override
    protected void addSchemataRecords()
    {
        QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogName());
        doQuery(ENDPOINT_SCHEMAS, prefix, ignore -> super.addSchemataRecords());
    }

    private void doQuery(GalaxyCacheEndpoint verb, QualifiedTablePrefix prefix, Consumer<QualifiedTablePrefix> backup)
    {
        URI uri = galaxyCacheClient.uriBuilder(session, prefix, verb, limit()).build();
        try {
            Iterator<List<Object>> rows = galaxyCacheClient.queryResults(session, prefix, verb, uri);
            while (rows.hasNext()) {
                List<Object> row = rows.next();
                Object[] values = row.stream()
                        .map(value -> {
                            if (value instanceof Integer intValue) {
                                return (long) intValue;
                            }
                            return value;
                        })
                        .toArray();
                addRecord(values);
            }
        }
        catch (NotFoundException ignore) {
            backup.accept(prefix);
        }
    }
}
