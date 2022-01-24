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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.trino.Session;
import io.trino.hdfs.TrinoFileSystemCache;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.plugin.objectstore.GalaxyIdentity.createIdentity;
import static io.trino.plugin.objectstore.MinioStorage.ACCESS_KEY;
import static io.trino.plugin.objectstore.MinioStorage.SECRET_KEY;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;

public class TestObjectStoreCorruptDeltaTable
        extends AbstractTestQueryFramework
{
    private MinioStorage minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(DELTA);
    }

    private QueryRunner createQueryRunner(TableType tableType)
            throws Exception
    {
        closeAfterClass(TrinoFileSystemCache.INSTANCE::closeAll);

        minio = closeAfterClass(new MinioStorage("test-bucket"));
        minio.start();

        TestingGalaxyMetastore metastore = closeAfterClass(new TestingGalaxyMetastore());

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> false));

        Session session = testSessionBuilder()
                .setIdentity(createIdentity(
                        "user",
                        new AccountId("a-12345678"),
                        new UserId("u-1234567890"),
                        new RoleId("r-1234567890"),
                        "testToken"))
                .setCatalog("objectstore")
                .setSchema("testschema")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        Map<String, String> properties = createObjectStoreProperties(
                tableType,
                locationSecurityServer.getClientConfig(),
                metastore.getMetastoreConfig(minio.getS3Url()),
                minio.getHiveS3Config());

        queryRunner.installPlugin(new ObjectStorePlugin());
        queryRunner.createCatalog("objectstore", "galaxy_objectstore", properties);
        queryRunner.createCatalog("anotherobjectstore", "galaxy_objectstore", properties);

        queryRunner.execute("CREATE SCHEMA objectstore.testschema");

        return queryRunner;
    }

    @Test
    public void testDropTableCorruptStorage()
    {
        String tableName = "corrupt_table";
        // create the table via `anotherobjectstore` to avoid adding it to the internal cache of Delta Lake connector from `objectstore` catalog
        assertUpdate(
                format("CREATE TABLE anotherobjectstore.testschema.%s (name VARCHAR(256), age INTEGER)", tableName));
        // break the table by deleting all its files including transaction log
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(minio.getEndpoint(), null))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .build();
        ObjectListing listing = s3.listObjects("test-bucket", "testschema/corrupt_table");
        s3.deleteObjects(new DeleteObjectsRequest("test-bucket")
                .withKeys(listing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).toArray(String[]::new))
                .withQuiet(true));

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }
}
