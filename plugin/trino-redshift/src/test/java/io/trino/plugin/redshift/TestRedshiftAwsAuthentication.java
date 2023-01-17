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
package io.trino.plugin.redshift;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.redshift.RedshiftQueryRunner.TEST_DATABASE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRedshiftAwsAuthentication
        extends AbstractTestQueryFramework
{
    private static final String CONNECTOR_NAME = "redshift";
    private static final String TEST_CATALOG = "redshift";
    private static final String TEST_SCHEMA = "test_iam_schema";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String jdbcUser = requireSystemProperty("test.redshift.jdbc.user");
        String jdbcPassword = requireSystemProperty("test.redshift.jdbc.password");
        String jdbcUrl = "jdbc:redshift://" + requireSystemProperty("test.redshift.jdbc.endpoint") + TEST_DATABASE;
        Jdbi dbi = Jdbi.create(jdbcUrl, jdbcUser, jdbcPassword);

        String iamJdbcUser = "iamtrino";
        createUserIfNotExists(dbi, iamJdbcUser, jdbcPassword);
        executeQueryWithRetry(dbi, handle -> handle.execute("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA));
        executeQuery(dbi, "CREATE OR REPLACE VIEW " + TEST_SCHEMA + ".view_current_user AS SELECT CURRENT_USER AS user");
        executeQueryWithRetry(dbi, handle -> handle.execute("GRANT ALL PRIVILEGES ON DATABASE " + TEST_DATABASE + " TO " + iamJdbcUser));
        executeQuery(dbi, "GRANT ALL PRIVILEGES ON SCHEMA " + TEST_SCHEMA + " TO " + iamJdbcUser);
        executeQuery(dbi, "GRANT SELECT ON ALL TABLES IN SCHEMA " + TEST_SCHEMA + " TO " + iamJdbcUser);

        DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession());
        DistributedQueryRunner runner = builder.build();
        Map<String, String> connectorProperties = Map.of(
                "connection-url", "jdbc:redshift:iam://" + requireSystemProperty("test.redshift.jdbc.endpoint") + TEST_DATABASE,
                "connection-user", iamJdbcUser,
                "redshift.authentication.type", "AWS",
                "aws.region-name", requireSystemProperty("test.redshift.aws.region"),
                "aws.access-key", requireSystemProperty("test.redshift.aws.access-key"),
                "aws.secret-key", requireSystemProperty("test.redshift.aws.secret-key"));
        try {
            runner.installPlugin(new RedshiftPlugin());
            runner.createCatalog(TEST_CATALOG, CONNECTOR_NAME, connectorProperties);
        }
        catch (Throwable e) {
            closeAllSuppress(e, runner);
            throw e;
        }
        return runner;
    }

    @Test
    public void testCurrentUser()
    {
        assertThat(query("SELECT CAST(trim(user) AS varchar) FROM " + TEST_SCHEMA + ".view_current_user"))
                .matches("VALUES (CAST('iamtrino' AS varchar))");
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(TEST_CATALOG)
                .setSchema(TEST_SCHEMA)
                .build();
    }

    private static String requireSystemProperty(String property)
    {
        return requireNonNull(System.getProperty(property), property + " is not set");
    }

    private static <E extends Exception> void executeQuery(Jdbi dbi, String sql)
            throws E
    {
        executeQuery(dbi, handle -> handle.execute(sql));
    }

    private static <E extends Exception> void executeQuery(Jdbi dbi, HandleConsumer<E> consumer)
            throws E
    {
        dbi.withHandle(consumer.asCallback());
    }

    private static <E extends Exception> void executeQueryWithRetry(Jdbi dbi, HandleConsumer<E> consumer)
    {
        Failsafe.with(RetryPolicy.builder()
                        .handleIf(e -> e.getMessage().matches(".* concurrent transaction .*"))
                        .withDelay(Duration.ofSeconds(10))
                        .withMaxRetries(3)
                        .build())
                .run(() -> executeQuery(dbi, consumer));
    }

    private static void createUserIfNotExists(Jdbi dbi, String user, String password)
    {
        try {
            executeQuery(dbi, handle -> handle.execute("CREATE USER " + user + " PASSWORD " + "'" + password + "'"));
        }
        catch (Exception e) {
            // if user already exists, swallow the exception
            if (!e.getMessage().matches(".*user \"" + user + "\" already exists.*")) {
                throw e;
            }
        }
    }
}
