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
package io.trino.plugin.warp2;

import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.objectstore.MinioStorage;
import io.trino.plugin.objectstore.ObjectStoreQueryRunner;
import io.trino.plugin.objectstore.TestingLocationSecurityServer;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestDynamicRowFiltering;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;

import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestWarpSpeedObjectIcebergDynamicRowFiltering
        extends AbstractTestDynamicRowFiltering
{
    private MinioStorage minio;
    private TestingGalaxyMetastore metastore;

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        metastore = null; // closed by closeAfterClass
        minio = null; // closed by closeAfterClass
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());
        minio = closeAfterClass(new MinioStorage("test-bucket-" + randomNameSuffix()));
        minio.start();

        metastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> !location.contains("denied")));
        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> galaxyCockroachContainer));

        DistributedQueryRunner queryRunner = ObjectStoreQueryRunner.builder()
                .withTableType(ICEBERG)
                .withAccountClient(testingAccountFactory.createAccountClient())
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getHiveS3Config())
                .withMetastore(metastore)
                .withLocationSecurityServer(locationSecurityServer)
                .withPlugin(WarpSpeedConnectorTestUtils.getPlugin())
                .withCoordinatorProperties(WarpSpeedConnectorTestUtils.getCoordinatorProperties())
                .withExtraObjectStoreProperties(WarpSpeedConnectorTestUtils.getProperties())
                .build();

        ObjectStoreQueryRunner.initializeTpchTables(queryRunner, REQUIRED_TPCH_TABLES);

        return queryRunner;
    }

    @Override
    protected SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        return ((DispatcherTableHandle) connectorTableHandle).getSchemaTableName();
    }
}
