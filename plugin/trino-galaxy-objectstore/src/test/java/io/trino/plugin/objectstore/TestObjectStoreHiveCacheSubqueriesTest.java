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

import io.trino.Session;
import io.trino.hdfs.TrinoFileSystemCache;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.spi.Plugin;
import io.trino.testing.BaseCacheSubqueriesTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class TestObjectStoreHiveCacheSubqueriesTest
        extends BaseCacheSubqueriesTest
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
        return createQueryRunner(
                Map.of(),
                Map.of());
    }

    protected final QueryRunner createQueryRunner(
            Map<String, String> coordinatorProperties,
            Map<String, String> extraObjectStoreProperties)
            throws Exception
    {
        closeAfterClass(TrinoFileSystemCache.INSTANCE::closeAll);

        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());
        minio = closeAfterClass(new MinioStorage("test-bucket"));
        minio.start();

        metastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> !location.contains("denied")));
        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> galaxyCockroachContainer));

        DistributedQueryRunner queryRunner = ObjectStoreQueryRunner.builder()
                .withTableType(HIVE)
                .withAccountClient(testingAccountFactory.createAccountClient())
                .withS3Url(minio.getS3Url())
                .withHiveS3Config(minio.getHiveS3Config())
                .withMetastore(metastore)
                .withLocationSecurityServer(locationSecurityServer)
                .withPlugin(getObjectStorePlugin())
                .withCoordinatorProperties(coordinatorProperties)
                .withExtraObjectStoreProperties(extraObjectStoreProperties)
                .withExtraProperties(EXTRA_PROPERTIES)
                .build();

        ObjectStoreQueryRunner.initializeTpchTables(queryRunner, REQUIRED_TABLES);

        return queryRunner;
    }

    @Override
    protected void createPartitionedTableAsSelect(String tableName, List<String> partitionColumns, String asSelect)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s WITH (partitioned_by=array[%s]) as %s",
                tableName,
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")),
                asSelect);

        getQueryRunner().execute(sql);
    }

    @Override
    protected Session withProjectionPushdownEnabled(Session session, boolean projectionPushdownEnabled)
    {
        return Session.builder(session)
                .setSystemProperty("objectstore.projection_pushdown_enabled", String.valueOf(projectionPushdownEnabled))
                .build();
    }

    protected Plugin getObjectStorePlugin()
    {
        return new ObjectStorePlugin();
    }
}
