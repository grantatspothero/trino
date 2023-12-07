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
package io.trino.connector.informationschema.galaxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.MediaType;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.ObjectMapperProvider;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.IdType;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.server.security.galaxy.GalaxyAccessControlConfig;
import io.trino.server.security.galaxy.GalaxyIdentity;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.airlift.http.client.testing.TestingResponse.contentType;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.STAT_CACHE_FAILURE;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheConstants.STAT_CACHE_SUCCESS;
import static io.trino.connector.informationschema.galaxy.GalaxyCacheSessionProperties.ENABLED;
import static io.trino.server.security.galaxy.GalaxyIdentity.GalaxyIdentityType.INDEXER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestGalaxyCache
        extends AbstractTestQueryFramework
{
    private static final GalaxyCacheConfig GALAXY_CACHE_CONFIG = new GalaxyCacheConfig().setEnabled(true).setSessionDefaultEnabled(true);
    private static final Identity IDENTITY = GalaxyIdentity.createIdentity("dummy", IdType.ACCOUNT_ID.randomId(AccountId::new), IdType.USER_ID.randomId(UserId::new), IdType.ROLE_ID.randomId(RoleId::new), "dummy", INDEXER);
    private static final Session SESSION = testSessionBuilder().setIdentity(IDENTITY).build();

    private AtomicReference<Supplier<Response>> response;
    private GalaxyCacheClient galaxyCacheClient;
    private ObjectMapper mapper;
    private GalaxyCacheStats stats;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        response = new AtomicReference<>();
        GalaxyAccessControlConfig accessControlConfig = new GalaxyAccessControlConfig().setAccountUri(URI.create("http://dummy.dummy"));
        stats = new GalaxyCacheStats();
        galaxyCacheClient = new GalaxyCacheClient(new TestingHttpClient(request -> response.get().get()), accessControlConfig, stats);
        mapper = new ObjectMapperProvider().get();

        LocalQueryRunner queryRunner = LocalQueryRunner.builder(SESSION)
                .withInformationSchemaPageSourceFactory((metadata, accessControl) -> new GalaxyCacheInformationSchemaPageSourceProvider(metadata, accessControl, galaxyCacheClient))
                .withTableCommentFactory((metadata, accessControl) -> new GalaxyCacheTableCommentSystemTable(galaxyCacheClient, metadata, accessControl))
                .withMaterializedViewFactory((metadata, accessControl) -> new GalaxyCacheMaterializedViewSystemTable(galaxyCacheClient, metadata, accessControl))
                .withExtraSystemSessionProperties(ImmutableSet.of(new GalaxyCacheSessionProperties(GALAXY_CACHE_CONFIG)))
                .build();
        MockConnectorFactory mockConnectorFactory = MockConnectorFactory.builder().build();
        queryRunner.createCatalog("test_catalog", mockConnectorFactory, ImmutableMap.of());
        return queryRunner;
    }

    @BeforeEach
    public void prepareTest()
    {
        response.set(null);
        stats.stats().clear();
    }

    @Test
    public void testSchemas()
    {
        ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
        builder.add(ImmutableList.of("catalog 1", "schema 1"));
        builder.add(ImmutableList.of("catalog 2", "schema 2"));
        setResponse(builder.build());

        assertThat(query("SELECT * FROM test_catalog.information_schema.schemata"))
                .matches(result -> result.getRowCount() == 2)
                .matches(assertRowFields(
                        new RowField(0, 0, "catalog 1"),
                        new RowField(0, 1, "schema 1"),
                        new RowField(1, 0, "catalog 2"),
                        new RowField(1, 1, "schema 2")));

        assertStat(STAT_CACHE_SUCCESS, 1);
        assertStat(STAT_CACHE_FAILURE, 0);
    }

    @Test
    public void testTables()
    {
        ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
        builder.add(ImmutableList.of("catalog 1", "schema 1", "table 1", "BASE TABLE", "dummy"));
        builder.add(ImmutableList.of("catalog 2", "schema 1", "table 1", "VIEW", "dummy"));
        setResponse(builder.build());

        assertThat(query("SELECT * FROM test_catalog.information_schema.tables"))
                .matches(result -> result.getRowCount() == 2)
                .matches(assertRowFields(
                        new RowField(0, 0, "catalog 1"),
                        new RowField(0, 1, "schema 1"),
                        new RowField(0, 2, "table 1"),
                        new RowField(0, 3, "BASE TABLE"),
                        new RowField(1, 0, "catalog 2"),
                        new RowField(1, 1, "schema 1"),
                        new RowField(1, 2, "table 1"),
                        new RowField(1, 3, "VIEW")));

        assertStat(STAT_CACHE_SUCCESS, 1);
        assertStat(STAT_CACHE_FAILURE, 0);
    }

    @Test
    public void testViews()
    {
        ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
        builder.add(ImmutableList.of("catalog 1", "schema 1", "table 1", "CREATE VIEW blah"));
        builder.add(ImmutableList.of("catalog 2", "schema 1", "table 1", "CREATE VIEW blim"));
        setResponse(builder.build());

        assertThat(query("SELECT * FROM test_catalog.information_schema.views"))
                .matches(result -> result.getRowCount() == 2)
                .matches(assertRowFields(
                        new RowField(0, 0, "catalog 1"),
                        new RowField(0, 1, "schema 1"),
                        new RowField(0, 2, "table 1"),
                        new RowField(0, 3, "CREATE VIEW blah"),
                        new RowField(1, 0, "catalog 2"),
                        new RowField(1, 1, "schema 1"),
                        new RowField(1, 2, "table 1"),
                        new RowField(1, 3, "CREATE VIEW blim")));

        assertStat(STAT_CACHE_SUCCESS, 1);
        assertStat(STAT_CACHE_FAILURE, 0);
    }

    @Test
    public void testMaterializedViews()
    {
        ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
        builder.add(Arrays.asList("test_catalog", "schema 1", "table 1", "storage catalog 1", "storage schema 1", "storage table 1", "STALE", null, "comment 1", "CREATE VIEW blah"));
        builder.add(Arrays.asList("test_catalog", "schema 2", "table 2", "storage catalog 2", "storage schema 2", "storage table 2", "UNKNOWN", null, "comment 2", "CREATE VIEW blah blah"));
        setResponse(builder.build());

        assertThat(query("SELECT * FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog'"))
                .matches(result -> result.getRowCount() == 2)
                .matches(assertRowFields(
                        new RowField(0, 0, "test_catalog"),
                        new RowField(0, 1, "schema 1"),
                        new RowField(0, 2, "table 1"),
                        new RowField(0, 3, "storage catalog 1"),
                        new RowField(0, 4, "storage schema 1"),
                        new RowField(0, 5, "storage table 1"),
                        new RowField(0, 6, "STALE"),
                        new RowField(0, 8, "comment 1"),
                        new RowField(0, 9, "CREATE VIEW blah"),
                        new RowField(1, 0, "test_catalog"),
                        new RowField(1, 1, "schema 2"),
                        new RowField(1, 2, "table 2"),
                        new RowField(1, 3, "storage catalog 2"),
                        new RowField(1, 4, "storage schema 2"),
                        new RowField(1, 5, "storage table 2"),
                        new RowField(1, 6, "UNKNOWN"),
                        new RowField(1, 8, "comment 2"),
                        new RowField(1, 9, "CREATE VIEW blah blah")));

        assertStat(STAT_CACHE_SUCCESS, 1);
        assertStat(STAT_CACHE_FAILURE, 0);
    }

    @Test
    public void testColumns()
    {
        ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
        builder.add(ImmutableList.of("catalog 1", "schema 1", "table 1", "column 1", 0, "dummy", "YES", "varchar", "a comment", "dummy", "also a comment"));
        builder.add(ImmutableList.of("catalog 1", "schema 1", "table 1", "column 2", 1, "dummy", "NO", "bigint", "another comment", "dummy", "also another comment"));
        setResponse(builder.build());

        assertThat(query("SELECT * FROM test_catalog.information_schema.columns"))
                .matches(result -> result.getRowCount() == 2)
                .matches(assertRowFields(
                        new RowField(0, 0, "catalog 1"),
                        new RowField(0, 1, "schema 1"),
                        new RowField(0, 2, "table 1"),
                        new RowField(0, 3, "column 1"),
                        new RowField(0, 4, 0L),
                        new RowField(0, 5, "dummy"),
                        new RowField(0, 6, "YES"),
                        new RowField(0, 7, "varchar"),
                        new RowField(1, 0, "catalog 1"),
                        new RowField(1, 1, "schema 1"),
                        new RowField(1, 2, "table 1"),
                        new RowField(1, 3, "column 2"),
                        new RowField(1, 4, 1L),
                        new RowField(1, 5, "dummy"),
                        new RowField(1, 6, "NO"),
                        new RowField(1, 7, "bigint")));

        assertStat(STAT_CACHE_SUCCESS, 1);
        assertStat(STAT_CACHE_FAILURE, 0);
    }

    @Test
    public void testComments()
    {
        ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
        builder.add(ImmutableList.of("catalog 1", "schema 1", "system", "BASE TABLE", "system comment 1"));
        builder.add(ImmutableList.of("catalog 2", "schema 1", "system", "VIEW", "system comment 2"));
        List<List<Object>> systemTableRows = builder.build();

        builder = ImmutableList.builder();
        builder.add(ImmutableList.of("catalog 1", "schema 1", "test_table", "BASE TABLE", "comment 1"));
        builder.add(ImmutableList.of("catalog 2", "schema 1", "test_table", "VIEW", "comment 2"));
        List<List<Object>> testTableRows = builder.build();

        AtomicInteger requestNumber = new AtomicInteger();
        Supplier<Response> testingResponse = () -> {
            // simulate querying different data sources
            List<List<Object>> rows = (requestNumber.getAndIncrement() == 0) ? systemTableRows : testTableRows;
            try {
                return new TestingResponse(HttpStatus.OK, contentType(MediaType.JSON_UTF_8), mapper.writeValueAsBytes(rows));
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
        response.set(testingResponse);

        assertThat(query("SELECT * FROM system.metadata.table_comments where table_name = 'test_table'"))
                .matches(result -> result.getRowCount() == 2)
                .matches(assertRowFields(
                        new RowField(0, 3, "comment 1"),
                        new RowField(1, 3, "comment 2")));

        assertStat(STAT_CACHE_SUCCESS, 1);
        assertStat(STAT_CACHE_FAILURE, 0);
    }

    @Test
    public void testCacheUnavailable()
    {
        response.set(() -> new TestingResponse(HttpStatus.NOT_FOUND, ArrayListMultimap.create(), new byte[0]));
        query("SELECT * FROM test_catalog.information_schema.schemata");

        assertStat(STAT_CACHE_SUCCESS, 0);
        assertStat(STAT_CACHE_FAILURE, 1);
    }

    @Test
    public void testSessionDisabled()
    {
        response.set(() -> new TestingResponse(HttpStatus.OK, contentType(MediaType.JSON_UTF_8), "[]".getBytes(StandardCharsets.UTF_8)));
        Session localSession = Session.builder(getSession()).setSystemProperty(ENABLED, "false").build();
        query(localSession, "SELECT * FROM test_catalog.information_schema.schemata");

        assertStat(STAT_CACHE_SUCCESS, 0);
        assertStat(STAT_CACHE_FAILURE, 1);
    }

    private void setResponse(List<List<Object>> rows)
    {
        Supplier<Response> testingResponse = () -> {
            try {
                return new TestingResponse(HttpStatus.OK, contentType(MediaType.JSON_UTF_8), mapper.writeValueAsBytes(rows));
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
        response.set(testingResponse);
    }

    private void assertStat(String statName, long count)
    {
        GalaxyCacheStats.Stat stat = stats.getStats("test_catalog").get(statName);
        assertNotNull(stat);
        assertEquals(stat.count().get(), count);
    }

    // need @SuppressWarnings("unused") as error-prone is broken here
    private record RowField(@SuppressWarnings("unused") int rowIndex, @SuppressWarnings("unused") int fieldIndex, @SuppressWarnings("unused") Object value) {}

    private Predicate<MaterializedResult> assertRowFields(RowField... rowFields)
    {
        return result -> Arrays.stream(rowFields)
                .allMatch(rowField -> result.getMaterializedRows().get(rowField.rowIndex()).getField(rowField.fieldIndex()).equals(rowField.value()));
    }
}
