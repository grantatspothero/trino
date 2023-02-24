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
package io.trino.plugin.hive;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.starburst.schema.discovery.SchemaDiscoveryController;
import io.starburst.schema.discovery.formats.orc.CopiedHdfsOrcDataSource;
import io.starburst.schema.discovery.formats.orc.OrcDataSourceFactory;
import io.starburst.schema.discovery.formats.parquet.CopiedHdfsParquetDataSource;
import io.starburst.schema.discovery.formats.parquet.ParquetDataSourceFactory;
import io.starburst.schema.discovery.trino.system.table.DiscoveryLocationAccessControlAdapter;
import io.starburst.schema.discovery.trino.system.table.SchemaDiscoverySystemTable;
import io.starburst.schema.discovery.trino.system.table.ShallowDiscoverySystemTable;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.util.Objects.requireNonNull;

public class SchemaDiscoverySystemTableProvider
        implements SystemTableProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final ExecutorService executor;
    private final ObjectMapper objectMapper;
    private final DiscoveryLocationAccessControlAdapter discoveryLocationAccessControlAdapter;

    @Inject
    public SchemaDiscoverySystemTableProvider(HdfsEnvironment hdfsEnvironment, ExecutorService executor, ObjectMapper objectMapper, LocationAccessControl locationAccessControl)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        requireNonNull(locationAccessControl, "locationAccessControl is null");
        this.discoveryLocationAccessControlAdapter = locationAccessControl::checkCanUseLocation;
    }

    @Override
    public Optional<SchemaTableName> getSourceTableName(SchemaTableName table)
    {
        return Optional.empty();
    }

    @Override
    public Optional<SystemTable> getSystemTable(HiveMetadata metadata, ConnectorSession session, SchemaTableName tableName)
    {
        if (tableName.equals(SchemaDiscoverySystemTable.SCHEMA_TABLE_NAME)) {
            SchemaDiscoveryController controller = createSchemaDiscoveryController(session);
            SchemaDiscoverySystemTable systemTable = new SchemaDiscoverySystemTable(controller, objectMapper, discoveryLocationAccessControlAdapter);
            return Optional.of(new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
        }
        if (tableName.equals(ShallowDiscoverySystemTable.SCHEMA_TABLE_NAME)) {
            SchemaDiscoveryController controller = createSchemaDiscoveryController(session);
            ShallowDiscoverySystemTable systemTable = new ShallowDiscoverySystemTable(controller, objectMapper, discoveryLocationAccessControlAdapter);
            return Optional.of(new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
        }
        return Optional.empty();
    }

    private SchemaDiscoveryController createSchemaDiscoveryController(ConnectorSession session)
    {
        HdfsContext hdfsContext = new HdfsContext(session);
        Function<URI, FileSystem> fileSystemProvider = uri -> {
            try {
                return hdfsEnvironment.getFileSystem(hdfsContext, new Path(uri));
            }
            catch (IOException e) {
                throw new TrinoException(INVALID_ARGUMENTS, "Invalid uri: " + uri, e);
            }
        };
        OrcDataSourceFactory orcDataSourceFactory = (id, size, options, inputStream) -> new CopiedHdfsOrcDataSource(id, size, options, inputStream, new FileFormatDataSourceStats());
        ParquetDataSourceFactory parquetDataSourceFactory = (id, estimatedSize, inputStream) -> new CopiedHdfsParquetDataSource(id, estimatedSize, inputStream, new FileFormatDataSourceStats(), new ParquetReaderOptions());
        return new SchemaDiscoveryController(fileSystemProvider, parquetDataSourceFactory, orcDataSourceFactory, executor);
    }
}
