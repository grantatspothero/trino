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
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient.GrantDetails;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.catalog.QueryCatalog;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.Version;
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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * @see TestGalaxyMetadataOnlyQueries
 */
// see comment at top of TestGalaxyQueries for debugging GalaxyQueryRunner queries in both Trino and Stargate portal-server!
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestMetadataOnlyQueries
        extends AbstractTestQueryFramework
{
    private static final JsonCodec<StatementRequest> STATEMENT_REQUEST_CODEC = jsonCodec(StatementRequest.class);
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private final UUID shutdownKey = UUID.randomUUID();

    private HttpClient httpClient;
    private File baseDir;
    private HiveMetastore metastore;
    private TestingAccountClient testingAccountClient;
    private CatalogId tpchCatalogId;
    private CatalogId hiveCatalogId;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        baseDir = Files.createTempDirectory(null).toFile();

        httpClient = new JettyHttpClient();

        metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                HDFS_FILE_SYSTEM_FACTORY,
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(baseDir.toURI().toString())
                        .setMetastoreUser("test"));

        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> closeAfterClass(new GalaxyCockroachContainer())));
        testingAccountClient = testingAccountFactory.createAccountClient();

        closeAfterClass(() -> {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
            baseDir = null;

            httpClient.close();
            httpClient = null;

            metastore = null;
            testingAccountClient = null;
        });

        tpchCatalogId = testingAccountClient.getOrCreateCatalog("tpch");
        hiveCatalogId = testingAccountClient.getOrCreateCatalog("hive");

        return GalaxyQueryRunner.builder()
                .setAccountClient(testingAccountClient)
                .addExtraProperty("catalog.management", "metadata_only")
                .addExtraProperty("trino.plane-id", "aws-us-east1-1")
                .addExtraProperty("kms-crypto.enabled", "false")
                .addExtraProperty("web-ui.authentication.type", "none")
                .setHttpAuthenticationType("galaxy-metadata")
                .addExtraProperty("galaxy.authentication.dispatch-token-issuer", "https://issuer.dispatcher.example.com")
                .addExtraProperty("experimental.concurrent-startup", "true")
                .addExtraProperty("metadata.shutdown.authentication-key", shutdownKey.toString())
                .addExtraProperty("metadata.shutdown.exit-delay", "999m")

                .addPlugin(new TpchPlugin())
                .addPlugin(new TestingHivePlugin(baseDir.toPath(), metastore))
                .setNodeCount(1)
                .setInstallSecurityModule(false)
                .setUseLiveCatalogs(false)
                .build();
    }

    @AfterEach
    public void verify()
    {
        MetadataOnlyTransactionManager transactionManager = getDistributedQueryRunner().getCoordinator().getInstance(Key.get(MetadataOnlyTransactionManager.class));
        assertThat(transactionManager.hasActiveTransactions()).isFalse();
    }

    @Test
    public void testSimpleSelect()
    {
        assertThat(queryMetadata("SELECT '123'"))
                .isEqualTo(matchResult("123"));
    }

    @Test
    public void testTpchSelect()
    {
        assertThat(queryMetadata("SELECT table_name, table_type FROM tpch.information_schema.tables LIMIT 1"))
                .isEqualTo(matchResult("applicable_roles", "BASE TABLE"));
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
                .isEqualTo(matchResult());
    }

    @Test
    public void testDdl()
    {
        testingAccountClient.setEntityOwnership(testingAccountClient.getAdminRoleId(), testingAccountClient.getAdminRoleId(), hiveCatalogId);
        testingAccountClient.grantFunctionPrivilege(new GrantDetails(Privilege.CREATE_SCHEMA, testingAccountClient.getAdminRoleId(), GrantKind.ALLOW, true, hiveCatalogId));

        assertThat(metastore.getAllDatabases()).isEmpty();
        queryMetadata("CREATE SCHEMA hive.test");
        assertThat(metastore.getAllDatabases()).containsExactly("test");
    }

    @Test
    public void testLateFailure()
    {
        assertThatThrownBy(() -> queryMetadata("SELECT fail(format('%s is too many', count(*))) FROM tpch.\"sf0.1\".orders"))
                .hasMessageMatching("\\QQueryError{message=150000 is too many, sqlState=null, errorCode=0, errorName=GENERIC_USER_ERROR, errorType=USER_ERROR, errorLocation=null, failureInfo=io.trino.client.FailureInfo@\\E\\w+\\Q}");
    }

    // must run last as it will shutdown the server, so named accordingly
    @Test
    public void testShutdown()
    {
        DistributedQueryRunner distributedQueryRunner = getDistributedQueryRunner();
        URI baseUrl = distributedQueryRunner.getCoordinator().getBaseUrl();

        // test with no key provided
        Request request = preparePut().setUri(baseUrl.resolve("/galaxy/metadata/v1/system/shutdown")).build();
        StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED.code());

        // test with invalid key provided
        request = preparePut().setHeader(HttpHeaders.AUTHORIZATION, "Bearer thisAintRight").setUri(baseUrl.resolve("/galaxy/metadata/v1/system/shutdown")).build();
        response = httpClient.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED.code());

        // test with invalid key format provided
        request = preparePut().setHeader(HttpHeaders.AUTHORIZATION, "garbage everywhere should fail").setUri(baseUrl.resolve("/galaxy/metadata/v1/system/shutdown")).build();
        response = httpClient.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED.code());

        // test with correct key provided
        request = preparePut().setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + shutdownKey).setUri(baseUrl.resolve("/galaxy/metadata/v1/system/shutdown")).build();
        response = httpClient.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT.code());
    }

    private MaterializedResult queryMetadata(@Language("SQL") String statement)
    {
        Request request = buildRequest(statement);
        return materialized(httpClient.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC)));
    }

    private Request buildRequest(String statement)
    {
        DistributedQueryRunner distributedQueryRunner = getDistributedQueryRunner();
        URI baseUrl = distributedQueryRunner.getCoordinator().getBaseUrl();

        List<QueryCatalog> catalogs = ImmutableList.of(
                new QueryCatalog(tpchCatalogId, new Version(1), "tpch", "tpch", true, ImmutableMap.of(), ImmutableMap.of(), Optional.empty(), Optional.empty()),
                new QueryCatalog(hiveCatalogId, new Version(1), "hive", "hive", false, ImmutableMap.of(), ImmutableMap.of(), Optional.empty(), Optional.empty()));

        StatementRequest statementRequest = new StatementRequest(testingAccountClient.getAccountId(), statement, catalogs, ImmutableMap.of(
                "access-control.name", "galaxy",
                "galaxy.account-url", testingAccountClient.getBaseUri().toString(),
                "galaxy.catalog-names", "tpch->" + tpchCatalogId.toString() + ",hive->" + hiveCatalogId,
                "galaxy.read-only-catalogs", "",
                "galaxy.shared-catalog-schemas", ""));

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
