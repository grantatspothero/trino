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
package io.trino.testing.containers.galaxy;

import org.jdbi.v3.core.Jdbi;
import org.testcontainers.containers.CockroachContainer;

import static java.lang.String.format;

public final class ExtendedCockroachContainer
        extends CockroachContainer
{
    private static final String COCKROACH_VERSION = "22.2";
    private static final String COCKROACH_CONTAINER = format("cockroachdb/cockroach:latest-v%s", COCKROACH_VERSION);
    private static final String REGION = "us-east1";
    private static final String CLUSTER_ORGANIZATION = "Starburst Dev";
    private static final String DEV_ENTERPRISE_LICENSE_KEY = "crl-0-EICrhrUGGAEiDVN0YXJidXJzdCBEZXY";

    public ExtendedCockroachContainer()
    {
        super(COCKROACH_CONTAINER);
        withCommand(format("start-single-node --insecure --locality=region=%s", REGION));
    }

    @Override
    protected void runInitScriptIfRequired()
    {
        super.runInitScriptIfRequired();
        Jdbi.create(() -> createConnection("")).useHandle(handle -> {
            handle.execute("SET CLUSTER SETTING \"cluster.organization\" = ?", CLUSTER_ORGANIZATION);
            handle.execute("SET CLUSTER SETTING \"enterprise.license\" = ?", DEV_ENTERPRISE_LICENSE_KEY);
            handle.execute("SET CLUSTER SETTING \"kv.closed_timestamp.lead_for_global_reads_override\" = '1ms'");
            handle.execute(format("ALTER DATABASE %s SET PRIMARY REGION \"%s\"", getDatabaseName(), REGION));
            handle.execute("CREATE DATABASE IF NOT EXISTS %s PRIMARY REGION \"%s\" REGIONS = \"%s\" SURVIVE ZONE FAILURE;"
                    .formatted(getDatabaseName() + "_query_history", REGION, REGION));
        });
    }
}
