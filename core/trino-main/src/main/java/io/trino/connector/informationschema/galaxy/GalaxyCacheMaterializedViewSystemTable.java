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

import com.google.inject.Inject;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.connector.system.MaterializedViewSystemTable;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import jakarta.ws.rs.NotFoundException;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.connector.informationschema.galaxy.GalaxyCacheEndpoint.ENDPOINT_MATERIALIZED_VIEWS;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheEndpoint.ENDPOINT_TABLES;
import static io.trino.connector.system.MaterializedViewSystemTable.TABLE_DEFINITION;
import static io.trino.connector.system.jdbc.FilterUtil.isImpossibleObjectName;
import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class GalaxyCacheMaterializedViewSystemTable
        implements SystemTable
{
    private final GalaxyCacheClient galaxyCacheClient;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final MaterializedViewSystemTable materializedViewSystemTable;

    @Inject
    public GalaxyCacheMaterializedViewSystemTable(GalaxyCacheClient galaxyCacheClient, Metadata metadata, AccessControl accessControl)
    {
        this.galaxyCacheClient = requireNonNull(galaxyCacheClient, "galaxyCacheClient is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.materializedViewSystemTable = new MaterializedViewSystemTable(metadata, accessControl);
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TABLE_DEFINITION;
    }

    @Override
    public RecordCursor cursor(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession connectorSession,
            TupleDomain<Integer> constraint)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        InMemoryRecordSet.Builder displayTable = InMemoryRecordSet.builder(getTableMetadata());

        Domain catalogDomain = constraint.getDomain(0, VARCHAR);
        Domain schemaDomain = constraint.getDomain(1, VARCHAR);
        Domain tableDomain = constraint.getDomain(2, VARCHAR);

        if (isImpossibleObjectName(catalogDomain) || isImpossibleObjectName(schemaDomain) || isImpossibleObjectName(tableDomain)) {
            return displayTable.build().cursor();
        }

        Optional<String> schemaFilter = tryGetSingleVarcharValue(schemaDomain);
        Optional<String> tableFilter = tryGetSingleVarcharValue(tableDomain);
        listCatalogNames(session, metadata, accessControl, catalogDomain).forEach(catalogName -> {
            QualifiedTablePrefix prefix = tablePrefix(catalogName, schemaFilter, tableFilter);

            URI uri = galaxyCacheClient.uriBuilder(session, prefix, ENDPOINT_MATERIALIZED_VIEWS, OptionalLong.empty()).addParameter("type", "BASE TABLE").build();
            try {
                Iterator<List<Object>> rows = galaxyCacheClient.queryResults(session, prefix, ENDPOINT_TABLES, uri);
                while (rows.hasNext()) {
                    List<Object> row = rows.next();
                    displayTable.addRow(row.toArray());
                }
            }
            catch (NotFoundException ignore) {
                materializedViewSystemTable.addMaterializedViewForCatalog(session, displayTable, prefix);
            }
        });

        return displayTable.build().cursor();
    }
}
