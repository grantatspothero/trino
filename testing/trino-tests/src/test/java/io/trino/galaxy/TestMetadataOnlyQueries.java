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
package io.trino.galaxy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient.GrantDetails;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.IdType;
import io.starburst.stargate.metadata.QueryCatalog;
import io.starburst.stargate.metadata.StatementRequest;
import io.trino.client.QueryResults;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.metadataonly.MetadataOnlyTransactionManager;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.sql.query.QueryAssertions.QueryAssert.newQueryAssert;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @see TestGalaxyMetadataOnlyQueries
 */
// see comment at top of TestGalaxyQueries for debugging GalaxyQueryRunner queries in both Trino and Stargate portal-server!
@Test(singleThreaded = true)
public class TestMetadataOnlyQueries
        extends AbstractTestQueryFramework
{
    private static final JsonCodec<StatementRequest> STATEMENT_REQUEST_CODEC = jsonCodec(StatementRequest.class);
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final CatalogId TPCH_CATALOG_ID = IdType.CATALOG_ID.randomId(CatalogId::new);
    private static final CatalogId HIVE_CATALOG_ID = IdType.CATALOG_ID.randomId(CatalogId::new);

    private HttpClient httpClient;
    private File baseDir;
    private HiveMetastore metastore;
    private TestingAccountClient testingAccountClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        baseDir = Files.createTempDirectory(null).toFile();

        httpClient = new JettyHttpClient();

        metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                HDFS_ENVIRONMENT,
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(baseDir.toURI().toString())
                        .setMetastoreUser("test"));

        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> closeAfterClass(new GalaxyCockroachContainer())));
        testingAccountClient = testingAccountFactory.createAccount();

        closeAfterClass(() -> {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
            baseDir = null;

            httpClient.close();
            httpClient = null;

            metastore = null;
            testingAccountClient = null;
        });

        return GalaxyQueryRunner.builder()
                .setAccountClient(testingAccountClient)
                .addExtraProperty("catalog.management", "metadata_only")
                .addExtraProperty("trino.plane-id", "aws-us-east1-1")
                .addExtraProperty("kms-crypto.enabled", "false")
                .addExtraProperty("web-ui.authentication.type", "none")
                .addExtraProperty("http-server.authentication.type", "galaxy-metadata")
                .addExtraProperty("galaxy.authentication.token-issuer", "https://local.gate0.net")
                .addExtraProperty("experimental.concurrent-startup", "true")
                .addPlugin(new TpchPlugin())
                .addPlugin(new TestingHivePlugin(metastore))
                .setNodeCount(1)
                .setInstallSecurityModule(false)
                .build();
    }

    @AfterMethod(alwaysRun = true)
    public void verify()
    {
        MetadataOnlyTransactionManager transactionManager = getDistributedQueryRunner().getCoordinator().getInstance(Key.get(MetadataOnlyTransactionManager.class));
        assertThat(transactionManager.hasActiveTransactions()).isFalse();
    }

    @Test
    public void testSimpleSelect()
    {
        assertThat(queryMetadata("SELECT '123'"))
                .skippingTypesCheck()
                .matches(matchResult("123"));
    }

    @Test
    public void testTpchSelect()
    {
        assertThat(queryMetadata("SELECT table_name, table_type FROM tpch.information_schema.tables LIMIT 1"))
                .skippingTypesCheck()
                .matches(matchResult("columns", "BASE TABLE"));
    }

    @Test
    public void testTpchEmptySelect()
    {
        assertThat(queryMetadata("""
                SELECT table_name, table_type
                FROM "tpch".information_schema.tables
                WHERE table_schema = 'information_schema' AND table_type IN ('VIEW')
                ORDER by table_name
                """))
                .skippingTypesCheck()
                .matches(matchResult());
    }

    @Test
    public void testDdl()
    {
        testingAccountClient.setEntityOwnership(testingAccountClient.getAdminRoleId(), testingAccountClient.getAdminRoleId(), HIVE_CATALOG_ID);
        testingAccountClient.grantFunctionPrivilege(new GrantDetails(Privilege.CREATE_SCHEMA, testingAccountClient.getAdminRoleId(), GrantKind.ALLOW, true, HIVE_CATALOG_ID));

        assertThat(metastore.getAllDatabases()).isEmpty();
        queryMetadata("CREATE SCHEMA hive.test");
        assertThat(metastore.getAllDatabases()).containsExactly("test");
    }

    private AssertProvider<QueryAssert> queryMetadata(@Language("SQL") String statement)
    {
        Request request = buildRequest(statement);
        MaterializedResult materializedResult = materialized(httpClient.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC)));
        return newQueryAssert(statement, getDistributedQueryRunner(), getSession(), materializedResult);
    }

    private Request buildRequest(String statement)
    {
        DistributedQueryRunner distributedQueryRunner = getDistributedQueryRunner();
        URI baseUrl = distributedQueryRunner.getCoordinator().getBaseUrl();

        List<QueryCatalog> catalogs = ImmutableList.of(
                new QueryCatalog("tpch", "tpch", ImmutableMap.of(), ImmutableMap.of(), Optional.empty()),
                new QueryCatalog("hive", "hive", ImmutableMap.of(), ImmutableMap.of(), Optional.empty()));

        StatementRequest statementRequest = new StatementRequest(testingAccountClient.getAccountId(), statement, catalogs, ImmutableMap.of(
                "access-control.name", "galaxy",
                "galaxy.account-url", testingAccountClient.getBaseUri().toString(),
                "galaxy.catalog-names", "tpch->" + TPCH_CATALOG_ID.toString() + ",hive->" + HIVE_CATALOG_ID,
                "galaxy.read-only-catalogs", ""));

        return preparePost().setUri(baseUrl.resolve("/galaxy/metadata/v1/statement"))
                .setHeader("X-Trino-User", "dummy")
                .addHeader("X-Trino-Extra-Credential", "userId=" + testingAccountClient.getAdminUserId())
                .addHeader("X-Trino-Extra-Credential", "roleId=" + testingAccountClient.getAdminRoleId())
                .setHeader("Content-Type", MediaType.APPLICATION_JSON_TYPE.withCharset(StandardCharsets.UTF_8.name()).toString())
                .setBodyGenerator(jsonBodyGenerator(STATEMENT_REQUEST_CODEC, statementRequest))
                .build();
    }

    private MaterializedResult materialized(QueryResults queryResults)
    {
        if (queryResults.getError() != null) {
            if (queryResults.getError().getFailureInfo() != null) {
                fail(queryResults.getError().toString(), queryResults.getError().getFailureInfo().toException());
            }
            fail(queryResults.getError().toString());
        }

        MaterializedResult.Builder resultBuilder = resultBuilder(getSession());

        if (queryResults.getData() == null) {
            return resultBuilder.build();
        }

        for (List<Object> row : queryResults.getData()) {
            resultBuilder.row(row.toArray());
        }

        return resultBuilder.build();
    }

    private MaterializedResult matchResult(Object... values)
    {
        MaterializedResult.Builder resultBuilder = resultBuilder(getSession());
        if (values.length > 0) {
            resultBuilder.row(values);
        }
        return resultBuilder.build();
    }
}
