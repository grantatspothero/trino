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
package io.trino.plugin.iceberg.catalog.file;

import io.starburst.stargate.metastore.client.BadMetastoreRequestException;
import io.starburst.stargate.metastore.client.MetastoreConflictException;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.hms.AbstractMetastoreTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Optional;

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;

@NotThreadSafe
public class GalaxyMetastoreTableOperations
        extends AbstractMetastoreTableOperations
{
    public GalaxyMetastoreTableOperations(
            FileIO fileIo,
            CachingHiveMetastore metastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, metastore, session, database, table, owner, location);
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version.orElseThrow() + 1);

        Table table = Table.builder(getTable())
                .setDataColumns(toHiveColumns(metadata.schema().columns()))
                .withStorage(storage -> storage.setLocation(metadata.location()))
                .setParameter(METADATA_LOCATION_PROP, newMetadataLocation)
                .setParameter(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation)
                .build();

        try {
            metastore.replaceTable(database, tableName, table, null);
        }
        catch (TrinoException e) {
            if (e.getCause() instanceof BadMetastoreRequestException) {
                throw e;
            }
            if (e.getCause() instanceof MetastoreConflictException) {
                throw new CommitFailedException(e, "%s", e.getCause().getMessage());
            }
            throw new CommitStateUnknownException(e);
        }
    }
}
