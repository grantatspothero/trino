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

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.BatchDeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.SchemaId;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.hive.metastore.glue.AwsApiCallStats;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.security.galaxy.GalaxyTestHelper;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.junit.jupiter.api.AfterAll;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.ALLOW;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;

/**
 * Test ObjectStore connector materialized views with Glue metastore.
 */
public class TestObjectStoreGlueMaterializedView
        extends BaseObjectStoreMaterializedViewTest
{
    private static final String TEST_CATALOG = "iceberg";

    private String schemaName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        GalaxyTestHelper galaxyTestHelper = closeAfterClass(new GalaxyTestHelper());
        galaxyTestHelper.initialize();

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> true));

        schemaName = "test_objectstore_iceberg_glue_" + randomNameSuffix();

        Map<String, String> properties = createObjectStoreProperties(
                ICEBERG,
                ImmutableMap.<String, String>builder()
                        .putAll(locationSecurityServer.getClientConfig())
                        .put("galaxy.catalog-id", "c-1234567890")
                        .buildOrThrow(),
                "glue",
                ImmutableMap.of("hive.metastore.glue.default-warehouse-dir", getSchemaDirectory()),
                ImmutableMap.of(),
                ImmutableMap.of());
        DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder(TEST_CATALOG, schemaName)
                .setAccountClient(galaxyTestHelper.getAccountClient())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new ObjectStorePlugin())
                .addCatalog(TEST_CATALOG, "galaxy_objectstore", false, properties)
                .addPlugin(new TpchPlugin())
                .addCatalog("tpch", "tpch", true, ImmutableMap.of())
                .build();
        queryRunner.execute("CREATE SCHEMA %s.%s".formatted(TEST_CATALOG, schemaName));
        queryRunner.execute("GRANT SELECT ON tpch.\"*\".\"*\" TO ROLE %s WITH GRANT OPTION".formatted(ACCOUNT_ADMIN));

        // Allows for tests in BaseIcebergMaterializedViewTest which use non_existent to check for not found
        galaxyTestHelper.getAccountClient()
                .grantFunctionPrivilege(new TestingAccountClient.GrantDetails(Privilege.CREATE_TABLE,
                        galaxyTestHelper.getAccountClient().getAdminRoleId(),
                        ALLOW,
                        false,
                        new SchemaId(galaxyTestHelper.getAccountClient().getOrCreateCatalog(TEST_CATALOG), "non_existent")));
        return queryRunner;
    }

    @Override
    protected String getSchemaDirectory()
    {
        return format("s3://galaxy-trino-ci/%s", schemaName);
    }

    @Override
    protected boolean isObjectStore()
    {
        return true;
    }

    @Override
    protected String getStorageMetadataLocation(String materializedViewName)
    {
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        Table table = glueClient.getTable(new GetTableRequest()
                        .withDatabaseName(schemaName)
                        .withName(materializedViewName))
                .getTable();
        return getTableParameters(table).get(METADATA_LOCATION_PROP);
    }

    @AfterAll
    public void cleanup()
    {
        cleanUpSchema(schemaName);
    }

    private static void cleanUpSchema(String schema)
    {
        AWSGlueAsync glueClient = AWSGlueAsyncClientBuilder.defaultClient();
        Set<String> tableNames = getPaginatedResults(
                glueClient::getTables,
                new GetTablesRequest().withDatabaseName(schema),
                GetTablesRequest::setNextToken,
                GetTablesResult::getNextToken,
                new AwsApiCallStats())
                .map(GetTablesResult::getTableList)
                .flatMap(Collection::stream)
                .map(Table::getName)
                .collect(toImmutableSet());
        glueClient.batchDeleteTable(new BatchDeleteTableRequest()
                .withDatabaseName(schema)
                .withTablesToDelete(tableNames));
        glueClient.deleteDatabase(new DeleteDatabaseRequest()
                .withName(schema));
    }

    @Override
    protected TableMetadata getStorageTableMetadata(String materializedViewName)
    {
        Location metadataLocation = Location.of(getStorageMetadataLocation(materializedViewName));
        TrinoFileSystem fileSystem = getTrinoFileSystem();
        return TableMetadataParser.read(new ForwardingFileIo(fileSystem), metadataLocation.toString());
    }

    private TrinoFileSystem getTrinoFileSystem()
    {
        ConfigurationInitializer s3Initializer = new TrinoS3ConfigurationInitializer(new HiveS3Config());
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(s3Initializer));
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, ImmutableSet.of());
        return new HdfsFileSystemFactory(new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication()), new TrinoHdfsFileSystemStats())
                .create(SESSION);
    }
}
