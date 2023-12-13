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
package io.trino.tests.product.launcher.suite.suites;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeHiveImpersonation;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeKerberosHiveImpersonation;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeKerberosHiveImpersonationWithCredentialCache;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class Suite5
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return testRuns().stream()
                // Galaxy does not use Hive impersonation
                .filter(run -> run.getEnvironment() != EnvSinglenodeHiveImpersonation.class)
                // Galaxy does not support Kerberos
                .filter(run -> !run.getEnvironment().getSimpleName().contains("Kerberos"))
                .collect(toImmutableList());
    }

    private List<SuiteTestRun> testRuns()
    {
        return ImmutableList.of(
                testOnEnvironment(EnvSinglenodeHiveImpersonation.class)
                        .withGroups("configured_features", "storage_formats", "hdfs_impersonation")
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHiveImpersonation.class)
                        .withGroups("configured_features", "storage_formats", "hdfs_impersonation", "authorization", "hive_kerberos")
                        .build(),
                testOnEnvironment(EnvSinglenodeKerberosHiveImpersonationWithCredentialCache.class)
                        .withGroups("configured_features", "storage_formats", "hdfs_impersonation", "authorization")
                        .build());
    }
}
