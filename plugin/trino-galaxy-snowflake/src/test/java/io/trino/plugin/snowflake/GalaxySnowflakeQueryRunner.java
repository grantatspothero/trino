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
package io.trino.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.starburstdata.trino.plugin.snowflake.SnowflakeServer;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.trino.plugin.objectstore.TestingLocationSecurityServer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.GalaxyQueryRunner;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.JDBC_URL;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.PASSWORD;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.ROLE;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.USER;
import static io.trino.server.security.galaxy.GalaxyTestHelper.ACCOUNT_ADMIN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class GalaxySnowflakeQueryRunner
{
    private static final String TEST_SCHEMA = "test_schema_2";
    private static final String CATALOG = "snowflake";

    private GalaxySnowflakeQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Map<String, String> connectorProperties;
        private SnowflakeServer server = new SnowflakeServer();
        private TestingLocationSecurityServer locationSecurityServer;
        private TestingAccountClient accountClient;
        private Optional<String> databaseName = Optional.empty();
        private Optional<String> warehouseName = Optional.empty();
        private String schemaName = TEST_SCHEMA;

        @CanIgnoreReturnValue
        public Builder withConnectorProperties(Map<String, String> properties)
        {
            this.connectorProperties = properties;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withServer(SnowflakeServer server)
        {
            this.server = requireNonNull(server, "server is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder withLocationSecurityServer(TestingLocationSecurityServer locationSecurityServer)
        {
            this.locationSecurityServer = locationSecurityServer;
            return this;
        }

        public Builder withAccountClient(TestingAccountClient accountClient)
        {
            this.accountClient = requireNonNull(accountClient, "accountClient is null");
            return this;
        }

        public Builder withWarehouse(Optional<String> warehouseName)
        {
            this.warehouseName = warehouseName;
            return this;
        }

        public Builder withDatabase(Optional<String> databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        public Builder withSchema(String schemaName)
        {
            this.schemaName = requireNonNull(schemaName, "schemaName is null");
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            // todo: reuse plugins query runner
            if (this.databaseName.isPresent()) {
                this.server.createSchema(this.databaseName.get(), this.schemaName);
            }

            ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
            properties.putAll(locationSecurityServer.getClientConfig());
            properties.put("snowflake.catalog-id", accountClient.getOrCreateCatalog(CATALOG).getCatalogId().toString());
            properties.put("galaxy.access-control-url", accountClient.getBaseUri().toString());
            properties.put("connection-url", JDBC_URL);
            properties.put("connection-user", USER);
            properties.put("connection-password", PASSWORD);
            warehouseName.ifPresent((warehouseName) -> {
                properties.put("snowflake.warehouse", warehouseName);
            });
            databaseName.ifPresent((databaseName) -> {
                properties.put("snowflake.database", databaseName);
            });
            properties.put("snowflake.role", ROLE);

            properties.putAll(connectorProperties);
            GalaxyQueryRunner.Builder builder = GalaxyQueryRunner.builder(CATALOG, schemaName);

            builder.setNodeCount(1);
            builder.addPlugin(new GalaxySnowflakePlugin());
            builder.addCatalog(CATALOG, "snowflake_parallel", false, properties.buildOrThrow());

            builder.setAccountClient(accountClient);

            DistributedQueryRunner queryRunner = builder.build();
            queryRunner.execute(format("GRANT CREATE ON SCHEMA %s.%s TO ROLE %s", CATALOG, TEST_SCHEMA, ACCOUNT_ADMIN));
            queryRunner.execute(format("GRANT SELECT ON %s.%s.\"*\" TO ROLE %s", CATALOG, TEST_SCHEMA, ACCOUNT_ADMIN));
            return queryRunner;
        }
    }
}
