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
package io.trino.server.security.galaxy;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.airlift.testing.TempFile;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingPortalClient;
import io.trino.server.galaxy.GalaxyCockroachContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.galaxy.GalaxyImageConstants.STARGATE_DOCKER_REPO;
import static io.trino.server.galaxy.GalaxyImageConstants.STARGATE_IMAGE_TAG;
import static io.trino.testing.containers.galaxy.PemUtils.getCertFile;
import static io.trino.testing.containers.galaxy.PemUtils.getHostNameFromPem;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMinutes;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;

public class DockerTestingAccountFactory
        implements TestingAccountFactory
{
    private static final Logger log = Logger.get(DockerTestingAccountFactory.class);

    public static final String PORTAL_SERVER_IMAGE = STARGATE_DOCKER_REPO + "portal-server:" + STARGATE_IMAGE_TAG;
    private static final String API_TESTING_CONFIG_FILE = "/tmp/config.properties";
    private static final String TRINO_TESTING_LAUNCHER_CONFIG_FILE = "/tmp/trino-testing-launcher.properties";
    private static final String TEST_CERT_PORTAL = "TEST_CERT_PORTAL";
    private static final int PORTAL_PORT = 8888;

    private final Closer closer = Closer.create();
    private final TestingPortalClient testingPortalClient;

    public DockerTestingAccountFactory(Optional<GalaxyCockroachContainer> providedCockroach)
    {
        GenericContainer<?> portalServer = null;
        try {
            GalaxyCockroachContainer cockroach = providedCockroach
                    .orElseGet(() -> closer.register(new GalaxyCockroachContainer()));

            // The pem is always available on the local file system
            File pemFile = getCertFile("portal");

            log.info("Running Trino Testing Portal Server");
            portalServer = new GenericContainer<>(PORTAL_SERVER_IMAGE);
            portalServer.setNetwork(cockroach.getNetwork());
            portalServer.addEnv(TEST_CERT_PORTAL, Files.readString(pemFile.toPath(), UTF_8));
            portalServer.addExposedPort(PORTAL_PORT);

            portalServer.setCommand(
                    "/opt/portal/bin/launcher",
                    "run",
                    "-Dhttp-server.https.port=" + PORTAL_PORT,
                    "--config", API_TESTING_CONFIG_FILE,
                    "--launcher-config", TRINO_TESTING_LAUNCHER_CONFIG_FILE);

            // Create and write API_TESTING_CONFIG_FILE
            writePropertiesFile(portalServer, API_TESTING_CONFIG_FILE, ImmutableMap.<String, String>builder()
                    .put("db.url", "jdbc:postgresql://cockroach:26257/postgres")
                    .put("db.user", cockroach.getUsername())
                    .put("db.password", cockroach.getPassword())
                    .buildOrThrow());

            // Create and write the TRINO_TESTING_LAUNCHER_CONFIG_FILE
            writePropertiesFile(portalServer, TRINO_TESTING_LAUNCHER_CONFIG_FILE, ImmutableMap.<String, String>builder()
                    .put("main-class", "io.starburst.stargate.portal.server.trinotest.TestingTrinoPortalServerMain")
                    .put("process-name", "portal-server")
                    .buildOrThrow());

            portalServer.waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(ofMinutes(2)));
            portalServer.start();

            String hostName = getHostNameFromPem(pemFile);
            URI portalServerUri = URI.create(format("https://%s:%s", hostName, portalServer.getMappedPort(PORTAL_PORT)));

            HttpClient httpClient = new JettyHttpClient();
            closer.register(httpClient);

            testingPortalClient = new TestingPortalClient(portalServerUri, httpClient);
        }
        catch (Throwable t) {
            if (portalServer != null) {
                log.error(t, "Revieved error during test, container log follows:\n%s", portalServer.getLogs());
            }
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    @Override
    public TestingAccountClient createAccount()
    {
        return testingPortalClient.createAccount();
    }

    private void writePropertiesFile(GenericContainer<?> portalServer, String containerPath, Map<String, String> properties)
            throws IOException
    {
        TempFile tempFile = closer.register(new TempFile());
        Files.write(
                tempFile.path(),
                properties.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(toImmutableList()),
                UTF_8);
        portalServer.withFileSystemBind(tempFile.path().toAbsolutePath().toString(), containerPath, BindMode.READ_ONLY);
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }
}
