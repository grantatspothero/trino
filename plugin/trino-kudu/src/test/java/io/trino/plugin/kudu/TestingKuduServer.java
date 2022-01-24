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
import com.google.common.net.HostAndPort;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static java.lang.String.format;
import static java.lang.Thread.sleep;

public class TestingKuduServer
        implements Closeable
{
    private static final Integer KUDU_MASTER_PORT = 7051;
    private static final Integer KUDU_TSERVER_PORT = 7050;
    private static final Integer NUMBER_OF_REPLICA = 4;

    private final GenericContainer<?> master;
    private final List<GenericContainer<?>> tServers;

    public TestingKuduServer()
    {
        Network network = Network.newNetwork();
        ImmutableList.Builder<GenericContainer<?>> tServersBuilder = ImmutableList.builder();
        int hostPort = 7051;
        String hostAlias = "kudu-master";

        // This is supposed to work but is not :shrug, see: https://github.com/docker/for-linux/issues/264#issuecomment-714253414
        // String hostDNS = "host.docker.internal";
        // Manually set, will automate in a bit: KUDU_QUICKSTART_IP=$(ifconfig | grep "inet " | grep -Fv 127.0.0.1 |  awk '{print $2}' | tail -1)
        String hostDNS = null;

        this.master = new FixedHostPortGenericContainer<>("apache/kudu:1.15.0")
                .withFixedExposedPort(hostPort, hostPort)
                .withCommand("master")
                .withEnv("KUDU_MASTERS", "kudu-master:7051")
                .withEnv("MASTER_ARGS", format("--fs_wal_dir=/var/lib/kudu/master --logtostderr --use_hybrid_clock=false --rpc_bind_addresses=%s:%s --rpc_advertised_addresses=%s:%s", "kudu-master", hostPort, hostDNS, hostPort))
                .withNetwork(network)
                .withNetworkAliases(hostAlias)
                .withExtraHost("host.docker.internal", "host-gateway");

        for (int instance = 0; instance < NUMBER_OF_REPLICA; instance++) {
            hostPort += 1;
            String instanceName = "kudu-tserver-" + instance;
            GenericContainer<?> tableServer = new FixedHostPortGenericContainer<>("apache/kudu:1.15.0")
                    .withFixedExposedPort(hostPort, hostPort)
                    .withCommand("tserver")
                    .withEnv("KUDU_MASTERS", "kudu-master:" + KUDU_MASTER_PORT)
                    .withNetwork(network)
                    .withNetworkAliases(instanceName)
                    .withExtraHost("host.docker.internal", "host-gateway")
                    .dependsOn(master)
                    .withEnv("TSERVER_ARGS", format("--fs_wal_dir=/var/lib/kudu/tserver --logtostderr --use_hybrid_clock=false --rpc_bind_addresses=%s:%s --rpc_advertised_addresses=%s:%s", instanceName, hostPort, hostDNS, hostPort));
            tServersBuilder.add(tableServer);
        }
        this.tServers = tServersBuilder.build();
        master.start();
        tServers.forEach(GenericContainer::start);

        // Workaround because sometimes the tablet servers will not be ready to start servicing requests
        try{
            sleep(30_000);
        }catch(Exception e){
            throw new RuntimeException(e);
        }

    }

    public HostAndPort getMasterAddress()
    {
        return HostAndPort.fromParts("localhost", 7051);
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            closer.register(master::stop);
            tServers.forEach(tabletServer -> closer.register(tabletServer::stop));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
