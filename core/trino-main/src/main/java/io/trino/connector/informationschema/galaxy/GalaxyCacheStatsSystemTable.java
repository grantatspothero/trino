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
import io.trino.security.AccessControl;
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

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.SCHEMA_NAME;
import static io.trino.connector.system.jdbc.FilterUtil.isImpossibleObjectName;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class GalaxyCacheStatsSystemTable
        implements SystemTable
{
    private static final String STATS_TABLE_NAME = "stats";

    private static final ConnectorTableMetadata TABLE_METADATA = new ConnectorTableMetadata(new SchemaTableName(SCHEMA_NAME, STATS_TABLE_NAME), ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("catalog_name", VARCHAR))
            .add(new ColumnMetadata("stat", VARCHAR))
            .add(new ColumnMetadata("count", BIGINT))
            .add(new ColumnMetadata("last_timestamp", TIMESTAMP_TZ_MILLIS))
            .build());

    private final GalaxyCacheStats stats;
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public GalaxyCacheStatsSystemTable(GalaxyCacheStats stats, Metadata metadata, AccessControl accessControl)
    {
        this.stats = requireNonNull(stats, "stats is null");
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
        Domain catalogDomain = constraint.getDomain(0, VARCHAR);

        Session session = ((FullConnectorSession) connectorSession).getSession();
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(TABLE_METADATA);
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();

        if (isImpossibleObjectName(catalogDomain)) {
            return table.build().cursor();
        }

        for (String catalog : listCatalogNames(session, metadata, accessControl, catalogDomain)) {
            stats.get(catalog).forEach(stat -> table.addRow(
                    catalog,
                    stat.name(),
                    stat.count().get(),
                    asTimestamp(timeZoneKey, stat.last())));
        }

        return table.build().cursor();
    }

    private Long asTimestamp(TimeZoneKey timeZoneKey, AtomicReference<Optional<Instant>> timestamp)
    {
        return timestamp.get().map(t -> packDateTimeWithZone(t.toEpochMilli(), timeZoneKey)).orElse(null);
    }
}
