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
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import jakarta.ws.rs.NotFoundException;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.ERROR_CACHE_IS_DISABLED;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.SCHEMA_NAME;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheEndpoint.ENDPOINT_STATUS;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheSessionProperties.isEnabled;
import static io.trino.connector.system.jdbc.FilterUtil.isImpossibleObjectName;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class GalaxyCacheStatusSystemTable
        implements SystemTable
{
    private static final String STATUS_TABLE_NAME = "status";

    private static final ConnectorTableMetadata TABLE_METADATA = new ConnectorTableMetadata(new SchemaTableName(SCHEMA_NAME, STATUS_TABLE_NAME), ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("catalog_name", VARCHAR))
            .add(new ColumnMetadata("status", VARCHAR))
            .add(new ColumnMetadata("details", VARCHAR))
            .add(new ColumnMetadata("timestamp", TIMESTAMP_TZ_MILLIS))
            .add(new ColumnMetadata("operational_message", VARCHAR))
            .add(new ColumnMetadata("active", BOOLEAN))
            .build());

    private final GalaxyCacheClient galaxyCacheClient;
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public GalaxyCacheStatusSystemTable(GalaxyCacheClient galaxyCacheClient, Metadata metadata, AccessControl accessControl)
    {
        this.galaxyCacheClient = requireNonNull(galaxyCacheClient, "galaxyCacheClient is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TABLE_METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();

        if (!isEnabled(session)) {
            throw new TrinoException(GENERIC_USER_ERROR, ERROR_CACHE_IS_DISABLED);
        }

        Domain catalogDomain = constraint.getDomain(0, VARCHAR);

        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(TABLE_METADATA);
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();

        if (isImpossibleObjectName(catalogDomain)) {
            return table.build().cursor();
        }

        for (String catalog : listCatalogNames(session, metadata, accessControl, catalogDomain)) {
            QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalog);
            URI uri = galaxyCacheClient.uriBuilder(session, prefix, ENDPOINT_STATUS, OptionalLong.empty()).build();
            try {
                Iterator<List<Object>> rows = galaxyCacheClient.queryResults(session, prefix, ENDPOINT_STATUS, uri);
                while (rows.hasNext()) {
                    List<Object> row = rows.next();
                    Long timestamp = (row.get(3) != null) ? packDateTimeWithZone((Long) row.get(3), timeZoneKey) : null;
                    table.addRow(row.get(0), row.get(1), row.get(2), timestamp, row.get(4), row.get(5));
                }
            }
            catch (NotFoundException ignore) {
                table.addRow(catalog, null, null, null, null, false);
            }
        }

        return table.build().cursor();
    }
}
