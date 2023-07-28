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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient.GrantDetails;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.catalog.EncryptedSecret;
import io.starburst.stargate.catalog.QueryCatalog;
import io.starburst.stargate.crypto.SecretEncryptionContext;
import io.starburst.stargate.crypto.SecretSealer;
import io.starburst.stargate.crypto.SecretSealer.SealedSecret;
import io.starburst.stargate.crypto.TestingMasterKeyCrypto;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.TrinoPlaneId;
import io.starburst.stargate.id.Version;
import io.starburst.stargate.metadata.StatementRequest;
import io.trino.client.QueryData;
import io.trino.client.QueryDataJsonSerializationModule;
import io.trino.client.QueryResults;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.objectstore.MinioStorage;
import io.trino.plugin.objectstore.ObjectStorePlugin;
import io.trino.plugin.objectstore.TableType;
import io.trino.plugin.objectstore.TestingLocationSecurityServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.metadataonly.CachingCatalogFactory;
import io.trino.server.metadataonly.MetadataOnlyConfig;
import io.trino.server.metadataonly.MetadataOnlyTransactionManager;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.core.MediaType;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePreDestroyCallback;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.starburst.stargate.crypto.TestingMasterKeyCrypto.PORTAL_KEY;
import static io.starburst.stargate.crypto.TestingMasterKeyCrypto.TRINO_KEY;
import static io.starburst.stargate.crypto.TestingMasterKeyCrypto.createPortalCrypto;
import static io.trino.plugin.objectstore.TestingObjectStoreUtils.createObjectStoreProperties;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Like {@link TestMetadataOnlyQueries} but uses (testing) Galaxy metastore and ObjectStore connector.
 *
 * @see TestMetadataOnlyQueries
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // Has verify() @AfterEach
@ExtendWith(TestGalaxyMetadataOnlyQueries.ConnectorShutdownServiceCheck.class)
public class TestGalaxyMetadataOnlyQueries
        extends AbstractTestQueryFramework
{
    private static final JsonCodec<StatementRequest> STATEMENT_REQUEST_CODEC = jsonCodec(StatementRequest.class);
    private static final io.trino.client.JsonCodec<QueryResults> QUERY_RESULTS_CODEC = io.trino.client.JsonCodec.jsonCodec(QueryResults.class, new QueryDataJsonSerializationModule());

    private TestingAccountClient testingAccountClient;
    private Map<String, String> objectStoreProperties;
    private HttpClient httpClient;
    private CatalogId tpchCatalogId;
    private CatalogId objectStoreCatalogId;
    private CachingCatalogFactory cachingCatalogFactory;
    private TrinoPlaneId trinoPlaneId;

    public static class ConnectorShutdownServiceCheck
            implements TestInstancePreDestroyCallback
    {
        @Override
        public void preDestroyTestInstance(ExtensionContext extensionContext)
        {
            extensionContext.getTestInstance().ifPresent(testInstance -> {
                TestGalaxyMetadataOnlyQueries testGalaxyMetadataOnlyQueries = (TestGalaxyMetadataOnlyQueries) testInstance;
                assertThat(testGalaxyMetadataOnlyQueries.cachingCatalogFactory.cacheSize()).isZero();
            });
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());
        MinioStorage minio = closeAfterClass(new MinioStorage("test-bucket-" + randomNameSuffix()));
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
                "galaxy",
                metastore.getMetastoreConfig(minio.getS3Url()),
                minio.getNativeS3Config(),
                Map.of());

        tpchCatalogId = testingAccountClient.getOrCreateCatalog("tpch");
        objectStoreCatalogId = testingAccountClient.getOrCreateCatalog("objectstore");

        DistributedQueryRunner queryRunner = GalaxyQueryRunner.builder()
                .setNodeCount(1)
                .setAccountClient(testingAccountClient)
                .setInstallSecurityModule(false) // MetadataOnlyCatalogManagerModule will install it
                .setUseLiveCatalogs(false)
                .addExtraProperty("catalog.management", "metadata_only")
                .addExtraProperty("web-ui.authentication.type", "none")
                .addExtraProperty("galaxy.authentication.dispatch-token-issuer", "https://issuer.dispatcher.example.com")
                .setHttpAuthenticationType("galaxy-metadata")
                .addExtraProperty("experimental.concurrent-startup", "true")
                .addExtraProperty("trino.plane-id", "aws-us-east1-1")
                .addExtraProperty("query.executor-pool-size", "3") // TODO shouldn't be needed. The pool should not grow to its limit when one query at a time
                .addPlugin(new TpchPlugin())
                .addPlugin(new IcebergPlugin())
                .addPlugin(new ObjectStorePlugin())
                .build();

        cachingCatalogFactory = queryRunner.getCoordinator().getInstance(Key.get(CachingCatalogFactory.class));
        trinoPlaneId = queryRunner.getCoordinator().getInstance(Key.get(MetadataOnlyConfig.class)).getTrinoPlaneId();

        return queryRunner;
    }

    @BeforeAll
    public void setUp()
            throws Exception
    {
        httpClient = closeAfterClass(new JettyHttpClient());

        testingAccountClient.setEntityOwnership(testingAccountClient.getAdminRoleId(), testingAccountClient.getAdminRoleId(), objectStoreCatalogId);
        testingAccountClient.grantFunctionPrivilege(new GrantDetails(Privilege.CREATE_SCHEMA, testingAccountClient.getAdminRoleId(), GrantKind.ALLOW, true, objectStoreCatalogId));

        queryMetadata("CREATE SCHEMA objectstore.default");
        for (TableType tableType : TableType.values()) {
            String createTable = "CREATE TABLE objectstore.default.metadata_object_store_table_" + tableType.name().toLowerCase(ENGLISH) + "(a integer) " +
                    "WITH (type = '" + tableType + "')";
            if (tableType != TableType.HUDI) {
                queryMetadata(createTable);
            }
            else {
                assertThatThrownBy(() -> queryMetadata(createTable))
                        .hasMessageMatching("\\QQueryError{message=Table creation is not supported for Hudi, sqlState=null, errorCode=13, errorName=NOT_SUPPORTED, errorType=USER_ERROR, errorLocation=null, failureInfo=io.trino.client.FailureInfo@\\E\\w+\\Q}");
            }
        }
    }

    @AfterAll
    public void tearDown()
    {
        httpClient = null; // closed by closeAfterClass
    }

    @AfterEach
    public void verify()
    {
        MetadataOnlyTransactionManager transactionManager = getDistributedQueryRunner().getCoordinator().getInstance(Key.get(MetadataOnlyTransactionManager.class));
        assertThat(transactionManager.hasActiveTransactions()).isFalse();
    }

    @Test
    public void testSimpleSelect()
            throws Exception
    {
        assertThat(queryMetadata("SELECT '123'"))
                .isEqualTo(matchResult("123"));
    }

    @Test
    public void testTpchSelect()
            throws Exception
    {
        assertThat(queryMetadata("SELECT table_name, table_type FROM tpch.information_schema.tables LIMIT 1"))
                .isEqualTo(matchResult("applicable_roles", "BASE TABLE"));
    }

    @Test
    public void testTpchEmptySelect()
            throws Exception
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
    public void testObjectStore()
            throws Exception
    {
        assertThat(queryMetadata("SHOW TABLES FROM objectstore.default"))
                .isEqualTo(resultBuilder(getSession())
                        .row("metadata_object_store_table_delta")
                        .row("metadata_object_store_table_hive")
                        .row("metadata_object_store_table_iceberg")
                        .build());

        assertThat(queryMetadata("SELECT table_name, column_name FROM objectstore.information_schema.columns WHERE table_name LIKE 'metada___object_store_table_%' "))
                .isEqualTo(resultBuilder(getSession())
                        .row("metadata_object_store_table_delta", "a")
                        .row("metadata_object_store_table_hive", "a")
                        .row("metadata_object_store_table_iceberg", "a")
                        .build());
    }

    @Test
    public void testLateFailure()
            throws Exception
    {
        assertThatThrownBy(() -> queryMetadata("SELECT fail(format('%s is too many', count(*))) FROM tpch.\"sf0.1\".orders"))
                .hasMessageMatching("\\QQueryError{message=150000 is too many, sqlState=null, errorCode=0, errorName=GENERIC_USER_ERROR, errorType=USER_ERROR, errorLocation=null, failureInfo=io.trino.client.FailureInfo@\\E\\w+\\Q}");
    }

    @Test
    public void testShowCreateMaterializedView()
            throws Exception
    {
        queryMetadata("CREATE MATERIALIZED VIEW objectstore.default.metadata_object_store_materialized_view_iceberg AS (SELECT * FROM objectstore.default.metadata_object_store_table_iceberg LIMIT 1)");
        assertThat(queryMetadata("SHOW CREATE MATERIALIZED VIEW objectstore.default.metadata_object_store_materialized_view_iceberg"))
                .isEqualTo(matchResult("""
                     CREATE MATERIALIZED VIEW objectstore.default.metadata_object_store_materialized_view_iceberg
                     WITH (
                        storage_schema = 'default'
                     ) AS
                     (
                        SELECT *
                        FROM
                          objectstore.default.metadata_object_store_table_iceberg
                        LIMIT 1
                     )"""));
    }

    private MaterializedResult queryMetadata(@Language("SQL") String statement)
            throws JsonProcessingException
    {
        Request request = buildRequest(statement);
        StringResponseHandler.StringResponse response = httpClient.execute(request, createStringResponseHandler());
        return materialized(QUERY_RESULTS_CODEC.fromJson(response.getBody()));
    }

    private Request buildRequest(String statement)
    {
        DistributedQueryRunner distributedQueryRunner = getDistributedQueryRunner();
        URI baseUrl = distributedQueryRunner.getCoordinator().getBaseUrl();

        // add a secret to test secrypt decryption

        // permissions won't allow us to use injected crypto instances. So create test versions from scratch.
        TestingMasterKeyCrypto portalCrypto = createPortalCrypto();
        SecretSealer secretSealer = new SecretSealer(portalCrypto);
        Map<String, String> portalEncryptionContext = SecretEncryptionContext.forPortal(testingAccountClient.getAccountId(), objectStoreCatalogId, "HIVE__galaxy.metastore.shared-secret");
        Map<String, String> verifierEncryptionContext = SecretEncryptionContext.forVerifier(testingAccountClient.getAccountId(), trinoPlaneId, Optional.of("objectstore"), "HIVE__galaxy.metastore.shared-secret");

        String hiveSecret = requireNonNull(objectStoreProperties.get("HIVE__galaxy.metastore.shared-secret"), "hiveSecret is null");
        SealedSecret portalSealedSecret = secretSealer.sealSecret(hiveSecret, PORTAL_KEY, portalEncryptionContext);
        SealedSecret reSealedSecret = secretSealer.resealSecret(portalSealedSecret, TRINO_KEY, portalEncryptionContext, verifierEncryptionContext);

        EncryptedSecret encryptedSecret = new EncryptedSecret("HIVE__galaxy.metastore.shared-secret", reSealedSecret.toString(), "XXXX_SECRET_XXXX");
        Map<String, String> encryptedObjectStoreProperties = new HashMap<>(objectStoreProperties);
        encryptedObjectStoreProperties.put("HIVE__galaxy.metastore.shared-secret", "XXXX_SECRET_XXXX");

        QueryCatalog tpch = new QueryCatalog(tpchCatalogId, new Version(1), "tpch", "tpch", true, Map.of(), Map.of(), Optional.empty(), Optional.empty());
        QueryCatalog objectStore = new QueryCatalog(objectStoreCatalogId, new Version(1), "objectstore", "galaxy_objectstore", false, encryptedObjectStoreProperties, Map.of(), Optional.of(ImmutableSet.of(encryptedSecret)), Optional.empty());
        List<QueryCatalog> catalogs = List.of(tpch, objectStore);

        TestingAccountClient testingAccountClient = getTestingAccountClient();
        StatementRequest statementRequest = new StatementRequest(testingAccountClient.getAccountId(), statement, catalogs, Map.of(
                "access-control.name", "galaxy",
                "galaxy.account-url", testingAccountClient.getBaseUri().toString(),
                "galaxy.catalog-names", "tpch->" + tpchCatalogId.toString() + ",objectstore->" + objectStoreCatalogId,
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

        QueryData queryData = queryResults.getData();
        if (queryData.isPresent()) {
            for (List<Object> row : queryData.getData()) {
                resultBuilder.row(row.toArray());
            }
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
