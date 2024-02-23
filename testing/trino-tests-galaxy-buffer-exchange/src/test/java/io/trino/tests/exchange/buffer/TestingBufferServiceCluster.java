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
package io.trino.tests.exchange.buffer;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.starburst.stargate.buffer.discovery.client.HttpDiscoveryClient;
import io.trino.testing.containers.wait.strategy.SelectedPortWaitStrategy;
import io.trino.util.AutoCloseableCloser;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HostAndPort.fromParts;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.output.OutputFrame.OutputType.END;

public class TestingBufferServiceCluster
        implements AutoCloseable
{
    private static final Logger log = Logger.get(TestingBufferServiceCluster.class);

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    private final DiscoveryServerContainer discoveryServerContainer;
    private final List<DataServerContainer> dataServerContainers;

    private TestingBufferServiceCluster(
            int dataServersCount,
            String discoveryServerImage,
            String dataServerImage)
    {
        try {
            log.info("Starting up buffer service cluster with %s data servers; using discoveryServerImage '%s' and dataServerImage '%s'",
                    dataServersCount,
                    discoveryServerImage,
                    dataServerImage);

            Network network = closer.register(Network.newNetwork());
            this.discoveryServerContainer = closer.register(new DiscoveryServerContainer(discoveryServerImage, "buffer-discovery-server", network));
            ImmutableList.Builder<DataServerContainer> dataServerContainersBuilder = ImmutableList.builder();
            for (int dataServer = 0; dataServer < dataServersCount; dataServer++) {
                dataServerContainersBuilder.add(closer.register(new DataServerContainer(
                        dataServerImage,
                        "buffer-data-server-" + dataServer,
                        "http://buffer-discovery-server:" + DiscoveryServerContainer.PORT,
                        network)));
            }
            this.dataServerContainers = dataServerContainersBuilder.build();

            discoveryServerContainer.start();
            for (DataServerContainer dataServerContainer : dataServerContainers) {
                dataServerContainer.start();
            }
            waitForCluster();
            log.info("Started testing buffer service cluster with " + dataServersCount + " data servers; discovery server endpoint is " + discoveryServerContainer.getDiscoveryApiEndpoint());
        }
        catch (Throwable startFailure) {
            log.error(startFailure, "Failed to start buffer service cluster");
            try {
                close();
            }
            catch (Exception closeFailure) {
                if (closeFailure != startFailure) {
                    startFailure.addSuppressed(closeFailure);
                }
            }
            throw startFailure;
        }
    }

    private void waitForCluster()
    {
        try (HttpClient httpClient = new JettyHttpClient()) {
            HttpDiscoveryClient client = new HttpDiscoveryClient(URI.create(getDiscoveryUri()), httpClient);

            RetryPolicy<Object> retryPolicy = RetryPolicy.builder()
                    .withMaxDuration(Duration.of(2, MINUTES))
                    .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                    .withDelay(Duration.of(2, SECONDS))
                    .build();
            Failsafe.with(retryPolicy).run(() -> checkState(client.getBufferNodes().bufferNodeInfos().size() == dataServerContainers.size(), "Not all data servers registered"));
        }
    }

    public String getDiscoveryUri()
    {
        return "http://%s".formatted(discoveryServerContainer.getDiscoveryApiEndpoint());
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private int dataServersCount = 3;
        private String discoveryServerImage;
        private String dataServerImage;

        public Builder() {}

        public Builder withDataServersCount(int dataServersCount)
        {
            checkArgument(dataServersCount > 0, "dataServersCount must be greater than 0");
            this.dataServersCount = dataServersCount;
            return this;
        }

        public Builder withImages(String registry, String version)
        {
            withDiscoveryServerImage(String.format("%s/discovery-server:%s", registry, version));
            withDataServerImage(String.format("%s/data-server:%s", registry, version));
            return this;
        }

        public Builder withDiscoveryServerImage(String discoveryServerImage)
        {
            this.discoveryServerImage = requireNonNull(discoveryServerImage, "discoveryServerImage is null");
            return this;
        }

        public Builder withDataServerImage(String dataServerImage)
        {
            this.dataServerImage = requireNonNull(dataServerImage, "dataServerImage is null");
            return this;
        }

        public TestingBufferServiceCluster build()
        {
            requireNonNull(discoveryServerImage, "Discovery server image not configured");
            requireNonNull(dataServerImage, "Data server image not configured");
            return new TestingBufferServiceCluster(dataServersCount, discoveryServerImage, dataServerImage);
        }
    }

    private static class DiscoveryServerContainer
            implements AutoCloseable
    {
        private final GenericContainer<?> container;

        private static final int PORT = 8080;

        protected DiscoveryServerContainer(String image, String hostName, Network network)
        {
            container = new GenericContainer<>(image);
            container.withNetwork(network);
            container.withLogConsumer(new PrintingLogConsumer(format("%-20s| ", hostName)));
            container.addExposedPort(PORT);
            container.withCreateContainerCmdModifier(c -> c.withHostName(hostName))
                    .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                    .waitingFor(new SelectedPortWaitStrategy(PORT))
                    .withStartupTimeout(Duration.ofMinutes(2));
        }

        public HostAndPort getDiscoveryApiEndpoint()
        {
            return fromParts(container.getHost(), container.getMappedPort(PORT));
        }

        public void start()
        {
            container.start();
        }

        @Override
        public void close()
        {
            container.close();
        }
    }

    private static class DataServerContainer
            implements AutoCloseable
    {
        public static final int MAX_START_ATTEMPTS = 5;
        private final String image;
        private final String hostName;
        private final String internalDiscoveryUri;
        private final Network network;

        private GenericContainer<?> container;

        DataServerContainer(String image, String hostName, String internalDiscoveryUri, Network network)
        {
            this.image = image;
            this.hostName = hostName;
            this.internalDiscoveryUri = internalDiscoveryUri;
            this.network = network;
        }

        @Override
        public void close()
        {
            if (container != null) {
                container.close();
                container = null;
            }
        }

        public void start()
        {
            checkState(this.container == null, "Container already created");
            for (int attempt = 0; attempt < MAX_START_ATTEMPTS; ++attempt) {
                int port = findFreeLocalPort();
                try {
                    this.container = createAndStartContainer(port);
                    break;
                }
                catch (Throwable e) {
                    if (isPortAlreadyInUseException(e) && attempt < MAX_START_ATTEMPTS - 1) {
                        log.warn("Port %s already used; trying once more", port);
                        continue;
                    }
                    throw e;
                }
            }
        }

        private static boolean isPortAlreadyInUseException(Throwable e)
        {
            return Throwables.getCausalChain(e).stream()
                    .anyMatch(t -> t.getMessage().contains("port is already allocated"));
        }

        private int findFreeLocalPort()
        {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                return socket.getLocalPort();
            }
            catch (IOException e) {
                throw new RuntimeException("Cannot find free local port");
            }
        }

        private FixedHostPortGenericContainer<?> createAndStartContainer(int port)
        {
            // We need to use FixedHostPortGenericContainer to have same port used by data-server container internally
            // and be mapped to localhost. This is required because the port used for communicating with data-server is passed
            // to client from discovery-server and data-server registration in discovery-server happens strictly within internal
            // docker network, without any awareness of port mapping.
            // We can work around the hostname problem registering each data-server as "localhost" but we have no way to overwrite
            // port used, it is taken from http-server.
            FixedHostPortGenericContainer<?> newContainer = new FixedHostPortGenericContainer<>(image);
            newContainer.withNetwork(network);
            newContainer.withLogConsumer(new PrintingLogConsumer(format("%-20s| ", hostName)));
            newContainer.withCopyToContainer(
                    Transferable.of("""
                             node.environment=test
                             http-server.http.port=%s
                             spooling.directory=file:///tmp
                             discovery-service.uri=%s
                             node.external-address=%s
                             """.formatted(port, internalDiscoveryUri, newContainer.getHost())),
                    "/opt/app/etc/config.properties");
            newContainer.withFixedExposedPort(port, port);
            newContainer.withCreateContainerCmdModifier(c -> c.withHostName(hostName))
                    .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                    .waitingFor(new SelectedPortWaitStrategy(port))
                    .withStartupTimeout(Duration.ofMinutes(2));

            newContainer.start();
            return newContainer;
        }
    }

    private static final class PrintingLogConsumer
            extends BaseConsumer<PrintingLogConsumer>
    {
        private static final Logger log = Logger.get(PrintingLogConsumer.class);

        private final String prefix;

        public PrintingLogConsumer(String prefix)
        {
            this.prefix = requireNonNull(prefix, "prefix is null");
        }

        @Override
        public void accept(OutputFrame outputFrame)
        {
            if (!log.isInfoEnabled()) {
                return;
            }
            // remove new line characters
            String message = outputFrame.getUtf8String().replaceAll("\\r?\\n?$", "");
            if (!message.isEmpty() || outputFrame.getType() != END) {
                log.info("%s%s", prefix, message);
            }
            if (outputFrame.getType() == END) {
                log.info("%s(exited)", prefix);
            }
        }
    }
}
