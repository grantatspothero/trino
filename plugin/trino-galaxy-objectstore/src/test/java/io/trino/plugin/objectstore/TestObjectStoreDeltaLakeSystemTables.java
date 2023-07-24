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

import io.trino.hdfs.TrinoFileSystemCache;
import io.trino.plugin.deltalake.TestDeltaLakeSystemTables;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.DistributedQueryRunner;

import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.CATALOG;
import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.TPCH_SCHEMA;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static java.lang.String.format;

public class TestObjectStoreDeltaLakeSystemTables
        extends TestDeltaLakeSystemTables
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        closeAfterClass(TrinoFileSystemCache.INSTANCE::closeAll);

        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());
        MinioStorage minio = closeAfterClass(new MinioStorage("test-bucket"));
        minio.start();

        TestingGalaxyMetastore metastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> true));
        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> galaxyCockroachContainer));

        DistributedQueryRunner queryRunner = ObjectStoreQueryRunner.builder()
                .withTableType(TableType.DELTA)
                .withAccountClient(testingAccountFactory.createAccountClient())
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getHiveS3Config())
                .withMetastore(metastore)
                .withLocationSecurityServer(locationSecurityServer)
                .withPlugin(new ObjectStorePlugin())
                .build();

        queryRunner.execute(format("GRANT SELECT ON \"" + CATALOG + "\".\"" + TPCH_SCHEMA + "\".\"*\" TO ROLE %s WITH GRANT OPTION", ACCOUNT_ADMIN));
        return queryRunner;
    }
}
