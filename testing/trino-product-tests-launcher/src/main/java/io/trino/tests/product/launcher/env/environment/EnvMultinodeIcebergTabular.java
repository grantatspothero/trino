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
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeIcebergTabular
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;

    @Inject
    public EnvMultinodeIcebergTabular(StandardMultinode standardMultinode, DockerFiles dockerFiles)
    {
        super(ImmutableList.of(standardMultinode));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String tabularCredential = requireNonNull(System.getenv("TABULAR_CREDENTIAL"), "environment variable not set: TABULAR_CREDENTIAL");
        String tabularWarehouse = requireNonNull(System.getenv("TABULAR_WAREHOUSE"), "environment variable not set: TABULAR_WAREHOUSE");

        builder.configureContainer(COORDINATOR, container -> container
                .withEnv("TABULAR_CREDENTIAL", tabularCredential)
                .withEnv("TABULAR_WAREHOUSE", tabularWarehouse));
        builder.configureContainer(WORKER, container -> container
                .withEnv("TABULAR_CREDENTIAL", tabularCredential)
                .withEnv("TABULAR_WAREHOUSE", tabularWarehouse));

        builder.addConnector("iceberg", forHostPath(dockerFiles.getDockerFilesHostPath(
                "conf/environment/multinode-iceberg-tabular/iceberg.properties")));
    }
}
