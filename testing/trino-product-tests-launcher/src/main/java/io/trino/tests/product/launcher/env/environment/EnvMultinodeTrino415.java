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
package io.trino.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.SupportedTrinoJdk.ZULU_17;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_ICEBERG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.wait.strategy.Wait.forHealthcheck;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeTrino415
        extends EnvironmentProvider
{
    private final PortBinder portBinder;
    private final DockerFiles dockerFiles;

    @Inject
    public EnvMultinodeTrino415(PortBinder portBinder, DockerFiles dockerFiles, StandardMultinode standardMultinode)
    {
        super(ImmutableList.of(standardMultinode));
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerFiles.ResourceProvider configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-trino-415");

        builder.configureContainer(TESTS, EnvMultinodeTrino415::exportAWSCredentials);

        builder.configureContainer(COORDINATOR, container -> container
                .withCopyFileToContainer(forHostPath(configDir.getPath("iceberg.properties")), CONTAINER_TRINO_ICEBERG_PROPERTIES)
                .withCopyFileToContainer(forHostPath(configDir.getPath("delta.properties")), CONTAINER_TRINO_ETC + "/catalog/delta.properties"));
        builder.configureContainer(COORDINATOR, EnvMultinodeTrino415::exportAWSCredentials);

        builder.configureContainer(WORKER, container -> container
                .withCopyFileToContainer(forHostPath(configDir.getPath("iceberg.properties")), CONTAINER_TRINO_ICEBERG_PROPERTIES)
                .withCopyFileToContainer(forHostPath(configDir.getPath("delta.properties")), CONTAINER_TRINO_ETC + "/catalog/delta.properties"));
        builder.configureContainer(WORKER, EnvMultinodeTrino415::exportAWSCredentials);

        DockerContainer container = new DockerContainer("trinodb/trino:415", "trino-415")
                // the server package is hundreds MB and file system bind is much more efficient
                .withEnv("JAVA_HOME", ZULU_17.getJavaHome())
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingForAll(forLogMessage(".*======== SERVER STARTED ========.*", 1), forHealthcheck())
                .withStartupTimeout(Duration.ofMinutes(5))
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/presto/etc/jvm.config")), "/etc/trino/jvm.config")
                .withCopyFileToContainer(forHostPath(configDir.getPath("iceberg.properties")), "/etc/trino/catalog/iceberg.properties")
                .withCopyFileToContainer(forHostPath(configDir.getPath("delta.properties")), "/etc/trino/catalog/delta.properties")
                .withExposedPorts(8080);
        exportAWSCredentials(container);
        portBinder.exposePort(container, 8070, 8080);
        builder.addContainer(container);

        configureTempto(builder, configDir);
    }

    private static void exportAWSCredentials(DockerContainer container)
    {
        // Either variable for setting the access key and secret key may be used.
        // This aligns with the AWS EnvironmentVariableCredentialsProvider.
        exportAWSCredential(container, "AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY");
        exportAWSCredential(container, "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY_ID");
        exportAWSCredential(container, "AWS_REGION");
    }

    private static void exportAWSCredential(DockerContainer container, String containerVariableName, String... allowedVariableNames)
    {
        Streams.concat(Stream.of(containerVariableName), Stream.of(allowedVariableNames))
                .map(System::getenv)
                .filter(Objects::nonNull)
                .findFirst()
                .ifPresentOrElse(
                        credentialValue -> container.withEnv(ImmutableMap.of(containerVariableName, credentialValue)),
                        () -> {
                            throw new IllegalStateException(format("Environment variable not set: %s", containerVariableName));
                        });
    }
}
