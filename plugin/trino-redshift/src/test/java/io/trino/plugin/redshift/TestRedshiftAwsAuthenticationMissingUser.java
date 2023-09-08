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

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.redshift.RedshiftQueryRunner.TEST_DATABASE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class TestRedshiftAwsAuthenticationMissingUser
        extends AbstractTestQueryFramework
{
    private static final String CONNECTOR_NAME = "redshift";
    private static final String TEST_CATALOG = "redshift";
    static final String TEST_SCHEMA = "test_schema";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession());
        DistributedQueryRunner runner = builder.build();
        Map<String, String> connectorProperties = Map.of(
                "connection-url", "jdbc:redshift:iam://" + requireSystemProperty("test.redshift.jdbc.endpoint") + TEST_DATABASE,
                "connection-user", "missinguser",
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
    public void testShowSchemasFails()
    {
        assertQueryFails("SHOW SCHEMAS", "Error listing schemas for catalog redshift: FATAL: user \"IAM:missinguser\" does not exist");
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
}
