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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.io.MoreFiles;
import com.google.common.net.HostAndPort;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class TestingKuduServer
        implements Closeable
{
    private String hostTmpDirectory;
    private String masterTmpDirectory;
    private List<String> tabletTmpDirectories;

    private static final Integer KUDU_MASTER_PORT = 7051;
    private static final Integer KUDU_TSERVER_PORT = 7050;
    private static final Integer NUMBER_OF_REPLICA = 3;

    private final GenericContainer<?> master;
    private final List<GenericContainer<?>> tServers;

    /**
     * WORKAROUND WARNING:
     * Kudu needs to know the host port it will be bound to in order to configure --rpc_advertised_addresses
     * However when using non-fixed ports in testcontainers, we only know the host/mapped port after the container starts up
     * In order to workaround this, we create volume mounts into each container and startup each container and have them watch the volume mounts
     * When the container starts up, we get the mapped port and write into the volume mount a special "mapped_port" file saying what the mapped port is
     * Then we can configure --rpc_advertised_addresses and startup kudu
     * Finally, since we never actually checked the containers started up correctly, we manually fire the HostPortWaitStrategy
     */
    public TestingKuduServer()
    {
        Network network = Network.newNetwork();
        ImmutableList.Builder<GenericContainer<?>> tServersBuilder = ImmutableList.builder();

        String hostIP = getHostIPAddress();
        createHostTmpDirectoriesForVolumeMounts();

        String masterContainerAlias = "kudu-master";
        this.master = new GenericContainer<>("apache/kudu:1.15.0")
                .withFileSystemBind(masterTmpDirectory, "/testcontainers")
                .withCopyFileToContainer(forClasspathResource("wait_until_port_bound.sh", 0777), "/wait_until_port_bound.sh")
                .withExposedPorts(KUDU_MASTER_PORT)
                .withCommand("/wait_until_port_bound.sh", "master")
                .withEnv("KUDU_MASTERS", "kudu-master:7051")
                // Note `rpc_advertised_addresses` does not have a port configured, it will be configured after container startup in `/wait_until_port_bound.sh`
                .withEnv("MASTER_TEMPLATE_ARGS", format("--fs_wal_dir=/var/lib/kudu/master --logtostderr --use_hybrid_clock=false --rpc_bind_addresses=%s:%s --rpc_advertised_addresses=%s:", masterContainerAlias, KUDU_MASTER_PORT, hostIP))
                .withNetwork(network)
                .withNetworkAliases(masterContainerAlias)
                // Do not wait, the host port will not be listening until after the container starts up and gets the mapped host port
                .waitingFor(new NoWaitStrategy());

        for (int instance = 0; instance < NUMBER_OF_REPLICA; instance++) {
            String instanceName = "kudu-tserver-" + instance;
            GenericContainer<?> tableServer = new GenericContainer<>("apache/kudu:1.15.0")
                    .withFileSystemBind(tabletTmpDirectories.get(instance), "/testcontainers")
                    .withCopyFileToContainer(forClasspathResource("wait_until_port_bound.sh", 0777), "/wait_until_port_bound.sh")
                    .withExposedPorts(KUDU_TSERVER_PORT)
                    .withCommand("/wait_until_port_bound.sh", "tserver")
                    .withEnv("KUDU_MASTERS", "kudu-master:" + KUDU_MASTER_PORT)
                    // Note `rpc_advertised_addresses` does not have a port configured, it will be configured after container startup in `/wait_until_port_bound.sh`
                    .withEnv("TSERVER_TEMPLATE_ARGS", format("--fs_wal_dir=/var/lib/kudu/tserver --logtostderr --use_hybrid_clock=false --rpc_bind_addresses=%s:%s --rpc_advertised_addresses=%s:", instanceName, KUDU_TSERVER_PORT, hostIP))
                    .withNetwork(network)
                    .withNetworkAliases(instanceName)
                    // Do not wait, the host port will not be listening until after the container starts up and gets the mapped host port
                    .waitingFor(new NoWaitStrategy())
                    .dependsOn(master);

            tServersBuilder.add(tableServer);
        }
        this.tServers = tServersBuilder.build();
        master.start();
        writeMappedPortToVolumeMount(masterTmpDirectory, master.getMappedPort(KUDU_MASTER_PORT));

        tServers.forEach(GenericContainer::start);
        IntStream.range(0, NUMBER_OF_REPLICA).forEach(replicaNumber -> writeMappedPortToVolumeMount(tabletTmpDirectories.get(replicaNumber), tServers.get(replicaNumber).getMappedPort(KUDU_TSERVER_PORT)));

        // Since we used NoWaitStategy, now ensure the master and workers are properly started up
        WaitStrategy waitStrategy = new HostPortWaitStrategy();
        waitStrategy.waitUntilReady(master);
        tServers.forEach(waitStrategy::waitUntilReady);

    }

    public HostAndPort getMasterAddress()
    {
        return HostAndPort.fromParts("localhost", master.getMappedPort(KUDU_MASTER_PORT));
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            MoreFiles.deleteRecursively(Paths.get(hostTmpDirectory), ALLOW_INSECURE);
            closer.register(master::stop);
            tServers.forEach(tabletServer -> closer.register(tabletServer::stop));
        }
        catch (FileSystemException e) {
            // Unfortunately, on CI environment, the user running file deletion runs into
            // access issues. However, the MoreFiles.deleteRecursively procedure may run successfully
            // on developer environments. So it makes sense to still have it so that cruft
            // isn't left behind.
            // TODO: Once access issues are resolved, remove this catch block.
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getHostIPAddress()
    {
        try {
            // This is roughly equivalent to setting the KUDU_QUICKSTART_IP defined here: https://kudu.apache.org/docs/quickstart.html#_set_kudu_quickstart_ip
            // What if there are multiple network interfaces with multiple IPV4 addresses bound? Choose one.
            return Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private void createHostTmpDirectoriesForVolumeMounts()
    {
        // Create tmp directories on the local filesystem, these will be used to communicate between host<=>container
        try{
            hostTmpDirectory = Files.createDirectory(Paths.get("/tmp/kudu-" + randomUUID()))
                    .toAbsolutePath().toString();
            masterTmpDirectory = Files.createDirectory(Paths.get(hostTmpDirectory, "master")).toAbsolutePath().toString();
            tabletTmpDirectories = IntStream.range(0, NUMBER_OF_REPLICA).boxed().map(replicaNumber -> {
                try {
                    return Files.createDirectories(Paths.get(hostTmpDirectory, "replica", String.valueOf(replicaNumber))).toAbsolutePath().toString();
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
        }
        catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    private static void writeMappedPortToVolumeMount(String tmpDirectory, int mappedPort)
    {
        File masterMappedPortFile = Paths.get(tmpDirectory, "mapped_port").toFile();
        try{
            FileUtils.writeStringToFile(masterMappedPortFile, String.valueOf(mappedPort), StandardCharsets.UTF_8);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    private static class NoWaitStrategy extends AbstractWaitStrategy {
        @Override
        public void waitUntilReady() {
        }
    }

}
