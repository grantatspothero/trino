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
package io.trino.plugin.iceberg.catalog.galaxy;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HiveSchemaProperties;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastore;
import io.trino.plugin.hive.metastore.galaxy.GalaxyHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.iceberg.IcebergSchemaProperties;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.iceberg.IcebergTableProperties.LOCATION_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTrinoGalaxyCatalog
        extends BaseTrinoCatalogTest
{
    private GalaxyCockroachContainer galaxyCockroachContainer;
    private TestingGalaxyMetastore galaxyMetastore;
    private Path tempDir;

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        TrinoFileSystemFactory fileSystemFactory = HDFS_FILE_SYSTEM_FACTORY;
        GalaxyHiveMetastore metastore = new GalaxyHiveMetastore(
                galaxyMetastore.getMetastore(),
                HDFS_FILE_SYSTEM_FACTORY,
                tempDir.toUri().toString(),
                new GalaxyHiveMetastoreConfig().isBatchMetadataFetch());
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        TestingTypeManager typeManager = new TestingTypeManager();
        return new TestableTrinoGalaxyCatalog(
                new CatalogName("catalog_name"),
                typeManager,
                cachingHiveMetastore,
                fileSystemFactory,
                new GalaxyMetastoreOperationsProvider(fileSystemFactory, typeManager, new IcebergGalaxyCatalogConfig()),
                useUniqueTableLocations,
                false);
    }

    @BeforeAll
    public void setUp()
            throws IOException
    {
        galaxyCockroachContainer = new GalaxyCockroachContainer();
        galaxyMetastore = new TestingGalaxyMetastore(galaxyCockroachContainer);
        tempDir = Files.createTempDirectory("test_trino_hive_catalog");
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        if (galaxyMetastore != null) {
            galaxyMetastore.close();
            galaxyMetastore = null;
        }
        if (galaxyCockroachContainer != null) {
            galaxyCockroachContainer.close();
            galaxyCockroachContainer = null;
        }
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Creating views is not supported");
    }

    @Override
    protected Map<String, Object> defaultNamespaceProperties(String newNamespaceName)
    {
        return Map.of(IcebergSchemaProperties.LOCATION_PROPERTY, "/tmp/path");
    }

    private static class TestableTrinoGalaxyCatalog
            extends TrinoGalaxyCatalog
    {
        private final CachingHiveMetastore metastore;
        private final TrinoFileSystemFactory fileSystemFactory;

        public TestableTrinoGalaxyCatalog(CatalogName catalogName, TypeManager typeManager, CachingHiveMetastore metastore, TrinoFileSystemFactory fileSystemFactory, IcebergTableOperationsProvider tableOperationsProvider, boolean useUniqueTableLocation, boolean cacheTableMetadata)
        {
            super(catalogName, typeManager, metastore, fileSystemFactory, tableOperationsProvider, useUniqueTableLocation, cacheTableMetadata);
            this.metastore = requireNonNull(metastore, "metastore is null");
            this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        }

        @Override
        public List<String> listNamespaces(ConnectorSession session)
        {
            return metastore.getAllDatabases().stream()
                    .filter(schemaName -> !HiveUtil.isHiveSystemSchema(schemaName))
                    .collect(toImmutableList());
        }

        @Override
        public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
        {
            // To make testNonLowercaseNamespace pass we need to lowercase the namespace
            // This should not be done in this place(preferably in metastore itself) if this method is moved to product
            String lowerCasedNamespace = namespace.toLowerCase(ENGLISH);
            Optional<Database> db = metastore.getDatabase(lowerCasedNamespace);
            if (db.isPresent()) {
                return HiveSchemaProperties.fromDatabase(db.get());
            }

            throw new SchemaNotFoundException(namespace);
        }

        @Override
        public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
        {
            // To make testNonLowercaseNamespace pass we need to lowercase the namespace
            // This should not be done in this place(preferably in metastore itself) if this method is moved to product
            String lowerCasedNamespace = namespace.toLowerCase(ENGLISH);
            Database.Builder database = Database.builder()
                    .setDatabaseName(lowerCasedNamespace)
                    .setOwnerType(Optional.of(owner.getType()))
                    .setOwnerName(Optional.of(owner.getName()));

            properties.forEach((property, value) -> {
                switch (property) {
                    case LOCATION_PROPERTY -> {
                        String location = (String) value;
                        try {
                            fileSystemFactory.create(session).directoryExists(Location.of(location));
                        }
                        catch (IOException | IllegalArgumentException e) {
                            throw new TrinoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + location, e);
                        }
                        database.setLocation(Optional.of(location));
                    }
                    default -> throw new IllegalArgumentException("Unrecognized property: " + property);
                }
            });

            metastore.createDatabase(database.build());
        }

        @Override
        public void dropNamespace(ConnectorSession session, String namespace)
        {
            // To make testNonLowercaseNamespace pass we need to lowercase the namespace
            // This should not be done in this place(preferably in metastore itself) if this method is moved to product
            String lowerCasedNamespace = namespace.toLowerCase(ENGLISH);
            // basic sanity check to provide a better error message
            if (!listTables(session, Optional.of(lowerCasedNamespace)).isEmpty()) {
                throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + namespace);
            }

            //always drop data
            metastore.dropDatabase(lowerCasedNamespace, true);
        }
    }
}
