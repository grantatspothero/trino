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
import io.airlift.log.Logger;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.connector.system.TableCommentSystemTable;
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

import static io.trino.connector.informationschema.galaxy.GalaxyCacheEndpoint.ENDPOINT_TABLES;
import static io.trino.connector.system.TableCommentSystemTable.COMMENT_TABLE;
import static io.trino.connector.system.jdbc.FilterUtil.isImpossibleObjectName;
import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class GalaxyCacheTableCommentSystemTable
        implements SystemTable
{
    private static final Logger log = Logger.get(GalaxyCacheTableCommentSystemTable.class);

    private final GalaxyCacheClient galaxyCacheClient;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final TableCommentSystemTable tableCommentSystemTable;

    @Inject
    public GalaxyCacheTableCommentSystemTable(GalaxyCacheClient galaxyCacheClient, Metadata metadata, AccessControl accessControl)
    {
        this.galaxyCacheClient = requireNonNull(galaxyCacheClient, "galaxyCacheClient is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        tableCommentSystemTable = new TableCommentSystemTable(metadata, accessControl);
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return COMMENT_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Domain catalogDomain = constraint.getDomain(0, VARCHAR);
        Domain schemaDomain = constraint.getDomain(1, VARCHAR);
        Domain tableDomain = constraint.getDomain(2, VARCHAR);

        Session session = ((FullConnectorSession) connectorSession).getSession();
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(COMMENT_TABLE);

        if (isImpossibleObjectName(catalogDomain) || isImpossibleObjectName(schemaDomain) || isImpossibleObjectName(tableDomain)) {
            return table.build().cursor();
        }

        Optional<String> schemaFilter = tryGetSingleVarcharValue(schemaDomain);
        Optional<String> tableFilter = tryGetSingleVarcharValue(tableDomain);
        for (String catalog : listCatalogNames(session, metadata, accessControl, tryGetSingleVarcharValue(catalogDomain))) {
            QualifiedTablePrefix prefix = tablePrefix(catalog, schemaFilter, tableFilter);

            URI uri = galaxyCacheClient.uriBuilder(session, prefix, ENDPOINT_TABLES, OptionalLong.empty()).addParameter("type", "BASE TABLE").build();
            try {
                Iterator<List<Object>> rows = galaxyCacheClient.queryResults(session, prefix, ENDPOINT_TABLES, uri);
                while (rows.hasNext()) {
                    List<Object> row = rows.next();
                    table.addRow(catalog, row.get(1), row.get(2), row.get(4));
                }
            }
            catch (NotFoundException ignore) {
                tableCommentSystemTable.addTableCommentForCatalog(session, prefix, catalog, table);
            }
            catch (Exception e) {
                // emulate TableCommentSystemTable and merely log exceptions
                log.warn(e, "Failed to get tables for catalog: %s", catalog);
            }
        }

        return table.build().cursor();
    }
}
