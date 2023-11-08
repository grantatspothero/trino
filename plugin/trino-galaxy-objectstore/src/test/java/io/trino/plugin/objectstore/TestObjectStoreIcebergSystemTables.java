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

import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.BaseIcebergSystemTables;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.BeforeAll;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.CATALOG;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestObjectStoreIcebergSystemTables
        extends BaseIcebergSystemTables
{
    public TestObjectStoreIcebergSystemTables()
    {
        super(PARQUET);
    }

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());
        MinioStorage minio = closeAfterClass(new MinioStorage("test-bucket-" + randomNameSuffix()));
        minio.start();

        TestingGalaxyMetastore metastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> true));
        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> galaxyCockroachContainer));

        return ObjectStoreQueryRunner.builder()
                .withTableType(TableType.ICEBERG)
                .withAccountClient(testingAccountFactory.createAccountClient())
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getHiveS3Config())
                .withMetastore(metastore)
                .withLocationSecurityServer(locationSecurityServer)
                .withPlugin(new ObjectStorePlugin())
                .build();
    }

    @Override
    @BeforeAll
    public void setUp()
    {
        super.setUp();
        getQueryRunner().execute(format("GRANT SELECT ON \"" + CATALOG + "\".\"test_schema\".\"*\" TO ROLE %s WITH GRANT OPTION", ACCOUNT_ADMIN));
    }
}
