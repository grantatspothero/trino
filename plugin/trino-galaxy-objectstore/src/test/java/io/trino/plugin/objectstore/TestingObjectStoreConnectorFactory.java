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
package io.trino.plugin.objectstore;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.TestingDeltaLakeExtensionsModule;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.objectstore.catalog.galaxy.TestingIcebergGalaxyMetastoreCatalogModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.objectstore.InternalObjectStoreConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingObjectStoreConnectorFactory
        implements ConnectorFactory
{
    private final HiveMetastore metastore;
    private final TrinoFileSystemFactory fileSystemFactory;

    public TestingObjectStoreConnectorFactory(HiveMetastore metastore, TrinoFileSystemFactory fileSystemFactory)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public String getName()
    {
        return "galaxy_objectstore";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(
                catalogName,
                config,
                Optional.of(metastore),
                Optional.of(fileSystemFactory),
                Optional.of(new TestingIcebergGalaxyMetastoreCatalogModule(metastore)),
                Optional.of(new TestingDeltaLakeMetastoreModule(metastore)),
                new TestingDeltaLakeExtensionsModule(),
                context);
    }
}