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
package io.trino.plugin.hive.metastore.galaxy;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.testing.TempFile;
import io.starburst.stargate.metastore.client.Metastore;
import io.starburst.stargate.metastore.client.MetastoreId;
import io.starburst.stargate.metastore.client.RestMetastore;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.containers.galaxy.PemUtils.getCertFile;
import static io.trino.testing.containers.galaxy.PemUtils.getHostNameFromPem;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.DriverManager.getConnection;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;

public class TestingGalaxyMetastore
        implements Closeable
{
    private static final String IMAGE_TAG = System.getProperty("stargate.version");
    private static final String DOCKER_REPO = "179619298502.dkr.ecr.us-east-1.amazonaws.com/galaxy/";
    private static final String METASTORE_IMAGE = DOCKER_REPO + "metastore-server:" + IMAGE_TAG;

    private final Closer closer = Closer.create();
    private final URI metastoreUri;
    private final Metastore metastore;
    private static final MetastoreId METASTORE_ID = new MetastoreId("ms-1234567890");
    private static final String SHARED_SECRET = "1234567890123456789012345678901234567890123456789012345678901234";

    public TestingGalaxyMetastore()
    {
        this(Optional.empty());
    }

    public TestingGalaxyMetastore(Optional<GalaxyCockroachContainer> providedCockroach)
    {
        try {
            GalaxyCockroachContainer cockroach = providedCockroach
                    .orElseGet(() -> closer.register(new GalaxyCockroachContainer()));

            GenericContainer<?> metastoreContainer = new GenericContainer<>(METASTORE_IMAGE);
            metastoreContainer.setNetwork(cockroach.getNetwork());
            metastoreContainer.addExposedPort(8443);
            metastoreContainer.setCommand(
                    "/opt/metastore/bin/launcher",
                    "run",
                    "--config", "/tmp/config.properties");

            File pemFile = getCertFile("service");
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("node.environment", "metastore")
                    .put("db.url", "jdbc:postgresql://cockroach:26257/postgres")
                    .put("db.user", cockroach.getUsername())
                    .put("db.password", cockroach.getPassword())
                    .put("http-server.http.enabled", "false")
                    .put("http-server.process-forwarded", "true")
                    .put("http-server.https.enabled", "true")
                    .put("http-server.https.keystore.path", pemFile.getAbsolutePath())
                    .put("http-server.https.keystore.key", "")
                    .buildOrThrow();
            TempFile propertiesFile = closer.register(new TempFile());

            Files.write(
                    propertiesFile.path(),
                    properties.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(toImmutableList()),
                    UTF_8);

            metastoreContainer.withFileSystemBind(propertiesFile.path().toAbsolutePath().toString(), "/tmp/config.properties", BindMode.READ_ONLY);
            metastoreContainer.withFileSystemBind(pemFile.getAbsolutePath(), pemFile.getPath(), BindMode.READ_ONLY);
            metastoreContainer.waitingFor(forLogMessage(".*SERVER STARTED.*", 1));
            metastoreContainer.start();
            closer.register(metastoreContainer::stop);

            try (Connection connection = getConnection(cockroach.getJdbcUrl(), cockroach.getUsername(), cockroach.getPassword())) {
                // drop the foreign keys to the accounts and data sources table to make testing easier
                connection.createStatement().execute("ALTER TABLE metastores DROP CONSTRAINT fk_account_id_ref_accounts");
                connection.createStatement().execute("ALTER TABLE metastores DROP CONSTRAINT fk_account_id_ref_catalogs_v2");
                verify(connection.createStatement().executeUpdate("" +
                        "INSERT INTO metastores (account_id, catalog_id, metastore_id, shared_secret) " +
                        "VALUES ('a-12345678', 'c-1234567890', '" + METASTORE_ID + "', '" + SHARED_SECRET + "')") == 1);
            }

            JettyHttpClient httpClient = closer.register(new JettyHttpClient());
            metastoreUri = URI.create("https://" + getHostNameFromPem(pemFile) + ":" + metastoreContainer.getFirstMappedPort());
            metastore = new RestMetastore(METASTORE_ID, SHARED_SECRET, metastoreUri, httpClient);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Metastore getMetastore()
    {
        return metastore;
    }

    public Map<String, String> getMetastoreConfig(String defaultDataDir)
    {
        return ImmutableMap.<String, String>builder()
                .put("galaxy.metastore.metastore-id", METASTORE_ID.toString())
                .put("galaxy.metastore.shared-secret", SHARED_SECRET)
                .put("galaxy.metastore.server-uri", metastoreUri.toString())
                .put("galaxy.metastore.default-data-dir", defaultDataDir)
                .buildOrThrow();
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }
}
