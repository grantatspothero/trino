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
import io.trino.hdfs.TrinoFileSystemCache;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.objectstore.MinioStorage;
import io.trino.plugin.objectstore.ObjectStorePlugin;
import io.trino.plugin.objectstore.TableType;
import io.trino.plugin.objectstore.TestingLocationSecurityServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.metadataonly.MetadataOnlyTransactionManager;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.sql.query.QueryAssertions.QueryAssert.newQueryAssert;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Like {@link TestMetadataOnlyQueries} but uses (testing) Galaxy metastore and ObjectStore connector.
 *
 * @see TestMetadataOnlyQueries
 */
@Test(singleThreaded = true) // Has verify() @AfterMethod
public class TestGalaxyMetadataOnlyQueries
        extends AbstractTestQueryFramework
{
    private static final JsonCodec<StatementRequest> STATEMENT_REQUEST_CODEC = jsonCodec(StatementRequest.class);
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final CatalogId TPCH_CATALOG_ID = IdType.CATALOG_ID.randomId(CatalogId::new);
    private static final CatalogId OBJECT_STORE_CATALOG_ID = IdType.CATALOG_ID.randomId(CatalogId::new);

    private TestingAccountClient testingAccountClient;
    private Map<String, String> objectStoreProperties;
    private HttpClient httpClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        closeAfterClass(TrinoFileSystemCache.INSTANCE::closeAll);

        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());
        MinioStorage minio = closeAfterClass(new MinioStorage("test-bucket"));
        minio.start();

        TestingGalaxyMetastore metastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));

        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> true));
        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> galaxyCockroachContainer));
        testingAccountClient = testingAccountFactory.createAccountClient();

        objectStoreProperties = createObjectStoreProperties(
                TableType.HIVE,
                ImmutableMap.<String, String>builder()
                        .putAll(locationSecurityServer.getClientConfig())
                        .put("galaxy.catalog-id", "c-1234567890")
                        .buildOrThrow(),
                metastore.getMetastoreConfig(minio.getS3Url()),
                minio.getHiveS3Config(),
                Map.of());

        return GalaxyQueryRunner.builder()
                .setNodeCount(1)
                .setAccountClient(testingAccountClient)
                .setInstallSecurityModule(false) // MetadataOnlyCatalogManagerModule will install it
                .addExtraProperty("catalog.management", "metadata_only")
                .addExtraProperty("web-ui.authentication.type", "none")
                .setHttpAuthenticationType("galaxy-metadata")
                .addExtraProperty("experimental.concurrent-startup", "true")
                .addExtraProperty("trino.plane-id", "aws-us-east1-1")
                .addExtraProperty("query.executor-pool-size", "3") // TODO shouldn't be needed. The pool should not grow to its limit when one query at a time
                .addPlugin(new TpchPlugin())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new ObjectStorePlugin())
                .build();
    }

    @BeforeClass
    public void setUp()
    {
        httpClient = closeAfterClass(new JettyHttpClient());

        testingAccountClient.setEntityOwnership(testingAccountClient.getAdminRoleId(), testingAccountClient.getAdminRoleId(), OBJECT_STORE_CATALOG_ID);
        testingAccountClient.grantFunctionPrivilege(new GrantDetails(Privilege.CREATE_SCHEMA, testingAccountClient.getAdminRoleId(), GrantKind.ALLOW, true, OBJECT_STORE_CATALOG_ID));

        queryMetadata("CREATE SCHEMA objectstore.default");
        for (TableType tableType : TableType.values()) {
            String createTable = "CREATE TABLE objectstore.default.metadata_object_store_table_" + tableType.name().toLowerCase(ENGLISH) + "(a integer) " +
                    "WITH (type = '" + tableType + "')";
            if (tableType != TableType.HUDI) {
                queryMetadata(createTable);
            }
            else {
                assertThatThrownBy(() -> queryMetadata(createTable))
                        .hasMessageContaining("Table creation is not supported for Hudi");
            }
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        httpClient = null; // closed by closeAfterClass
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
    public void testObjectStore()
    {
        assertThat(queryMetadata("SHOW TABLES FROM objectstore.default"))
                .matches(resultBuilder(getSession())
                        .row("metadata_object_store_table_hive")
                        .row("metadata_object_store_table_iceberg")
                        .row("metadata_object_store_table_delta")
                        .build());

        assertThat(queryMetadata("SELECT table_name, column_name FROM objectstore.information_schema.columns WHERE table_name LIKE 'metada___object_store_table_%' "))
                .matches(resultBuilder(getSession())
                        .row("metadata_object_store_table_hive", "a")
                        .row("metadata_object_store_table_iceberg", "a")
                        .row("metadata_object_store_table_delta", "a")
                        .build());
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

        List<QueryCatalog> catalogs = List.of(
                new QueryCatalog("tpch", "tpch", Map.of(), Map.of(), Optional.empty()),
                new QueryCatalog("objectstore", "galaxy_objectstore", objectStoreProperties, Map.of(), Optional.empty()));

        TestingAccountClient testingAccountClient = getTestingAccountClient();
        StatementRequest statementRequest = new StatementRequest(testingAccountClient.getAccountId(), statement, catalogs, Map.of(
                "access-control.name", "galaxy",
                "galaxy.account-url", testingAccountClient.getBaseUri().toString(),
                "galaxy.catalog-names", "tpch->" + TPCH_CATALOG_ID.toString() + ",objectstore->" + OBJECT_STORE_CATALOG_ID,
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

    private TestingAccountClient getTestingAccountClient()
    {
        TestingTrinoServer coordinator = this.getDistributedQueryRunner().getCoordinator();
        return coordinator.getInstance(Key.get(TestingAccountClient.class));
    }
}
