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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.hive.metastore.MetastoreMethod;
import io.trino.plugin.hive.metastore.galaxy.TestingGalaxyMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import io.trino.server.security.galaxy.TestingAccountFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.hive.metastore.MetastoreInvocations.assertMetastoreInvocationsForQuery;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_DATABASES;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_TABLES;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_VIEWS;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLES;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLES_WITH_PARAMETER;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_VIEWS;
import static io.trino.plugin.hive.metastore.MetastoreMethod.STREAM_TABLES;
import static io.trino.server.security.galaxy.TestingAccountFactory.createTestingAccountFactory;
import static java.nio.file.Files.createTempDirectory;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Similar to {@link io.trino.plugin.hive.metastore.thrift.TestHiveMetastoreMetadataQueriesAccessOperations},
 * but it doesn't extend the class because the access count differs. TODO this class currently does not set up any test schemas/tables.
 * <p>
 * Similar to {@link TestObjectStoreFileAndMetastoreAccessOperations}.
 *
 * @see TestObjectStoreFileAndMetastoreAccessOperations
 */
@Execution(SAME_THREAD) // metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestObjectStoreGalaxyMetastoreMetadataQueriesAccessOperations
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "objectstore";
    private static final String SCHEMA_NAME = "test_schema";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path schemaDirectory = createTempDirectory(null);
        GalaxyCockroachContainer galaxyCockroachContainer = closeAfterClass(new GalaxyCockroachContainer());

        TestingGalaxyMetastore galaxyMetastore = closeAfterClass(new TestingGalaxyMetastore(galaxyCockroachContainer));
        TestingLocationSecurityServer locationSecurityServer = closeAfterClass(new TestingLocationSecurityServer((session, location) -> true));

        TestingAccountFactory testingAccountFactory = closeAfterClass(createTestingAccountFactory(() -> galaxyCockroachContainer));

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("DELTA__delta.enable-non-concurrent-writes", "true")
                // Galaxy uses metastore cache by default, but disabling it for verifying the raw behavior
                .put("HIVE__hive.metastore-cache-ttl", "0s")
                .put("DELTA__hive.metastore-cache-ttl", "0s")
                .put("HUDI__hive.metastore-cache-ttl", "0s")
                .buildOrThrow();
        DistributedQueryRunner queryRunner = ObjectStoreQueryRunner.builder()
                .withCatalogName(CATALOG_NAME)
                .withSchemaName(SCHEMA_NAME)
                .withAccountClient(testingAccountFactory.createAccountClient())
                .withLocationSecurityServer(locationSecurityServer)
                .withMetastore(galaxyMetastore)
                .withS3Url(schemaDirectory.toUri().toString())
                .withTableType(new ObjectStoreConfig().getTableType())
                .withExtraObjectStoreProperties(properties)
                .withHiveS3Config(Map.of())
                .build();
        return queryRunner;
    }

    @Test
    public void testSelectSchemasWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.schemata", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectSchemasWithFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.schemata WHERE schema_name = 'information_schema'", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas WHERE table_schem = 'information_schema'", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectSchemasWithLikeOverSchemaName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.schemata WHERE schema_name LIKE 'test%'", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas WHERE table_schem LIKE 'test%'", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectTablesWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectTablesWithFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectTablesWithFilterBySchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLES)
                        .add(GET_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem = 'test_schema_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLES)
                        .add(GET_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectTablesWithLikeOverSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLES)
                        .add(GET_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectTablesWithFilterByTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_name = 'test_table_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name = 'test_table_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectTablesWithLikeOverTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_name LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLES)
                        .add(GET_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectViewsWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_VIEWS)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectViewsWithFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectViewsWithFilterBySchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema = 'test_schema_0'", ImmutableMultiset.of(GET_VIEWS));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem = 'test_schema_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_TABLES)
                        .add(GET_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectViewsWithLikeOverSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_VIEWS)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectViewsWithFilterByTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_name = 'test_table_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLE)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name = 'test_table_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectViewsWithLikeOverTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_name LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_VIEWS)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .add(GET_TABLES_WITH_PARAMETER)
                        .build());
    }

    @Test
    public void testSelectColumnsWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(STREAM_TABLES)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(STREAM_TABLES)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectColumnsFilterBySchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema = 'test_schema_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(STREAM_TABLES)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem = 'test_schema_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(STREAM_TABLES)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test\\_schema\\_0' ESCAPE '\\'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(STREAM_TABLES)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test_schema_0' ESCAPE '\\'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLES)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(STREAM_TABLES)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_name = 'test_table_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_TABLE, 2)
                        .build());

        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name = 'test_table_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLES)
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLES)
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLES)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_name LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLES)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLES)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByColumn()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE column_name = 'name'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(STREAM_TABLES)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE column_name = 'name'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(STREAM_TABLES)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverColumn()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE column_name LIKE 'n%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_TABLES)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE column_name LIKE 'n%'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .add(STREAM_TABLES)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByTableAndSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema = 'test_schema_0' AND table_name = 'test_table_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem = 'test_schema_0' AND table_name = 'test_table_0'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test\\_schema\\_0' ESCAPE '\\' AND table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test_schema_0' ESCAPE '\\' AND table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.<MetastoreMethod>builder()
                        .add(GET_ALL_DATABASES)
                        .build());
    }

    private void assertMetastoreInvocations(@Language("SQL") String query, Multiset<MetastoreMethod> expectedInvocations)
    {
        assertMetastoreInvocationsForQuery(getDistributedQueryRunner(), getDistributedQueryRunner().getDefaultSession(), query, expectedInvocations);
    }
}
