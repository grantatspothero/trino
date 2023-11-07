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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.NATION;

public class TestObjectStoreHudiSystemTables
        extends AbstractTestQueryFramework
{
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

        DistributedQueryRunner queryRunner = ObjectStoreQueryRunner.builder()
                .withTableType(TableType.HUDI)
                .withAccountClient(testingAccountFactory.createAccountClient())
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getHiveS3Config())
                .withMetastore(metastore)
                .withLocationSecurityServer(locationSecurityServer)
                .withPlugin(new ObjectStorePlugin())
                .build();

        ObjectStoreQueryRunner.initializeTpchTablesHudi(queryRunner, ImmutableList.of(NATION), metastore);
        queryRunner.execute("GRANT SELECT ON objectstore.tpch.\"*\" TO ROLE " + ACCOUNT_ADMIN);
        return queryRunner;
    }

    @Test
    public void testTimelineTable()
    {
        assertQuery("SHOW COLUMNS FROM tpch.\"nation$timeline\"",
                "VALUES ('timestamp', 'varchar', '', '')," +
                        "('action', 'varchar', '', '')," +
                        "('state', 'varchar', '', '')");

        assertQuery("SELECT timestamp, action, state FROM tpch.\"nation$timeline\"",
                "VALUES ('0', 'commit', 'COMPLETED')");

        assertQueryFails("SELECT timestamp, action, state FROM tpch.\"orders$timeline\"",
                ".*Table 'objectstore.tpch.\"orders\\$timeline\"' does not exist");
    }
}
