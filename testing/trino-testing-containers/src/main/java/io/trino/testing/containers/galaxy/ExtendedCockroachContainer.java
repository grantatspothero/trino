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

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.jdbi.v3.core.Jdbi;
import org.testcontainers.containers.CockroachContainer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.temporal.ChronoUnit;

import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.http.HttpClient.newHttpClient;

public final class ExtendedCockroachContainer
        extends CockroachContainer
{
    private static final String COCKROACH_VERSION = "21.2";
    private static final String COCKROACH_CONTAINER = format("cockroachdb/cockroach:latest-v%s", COCKROACH_VERSION);
    private static final String COCKROACH_LICENSE_URL = "https://register.cockroachdb.com/api/license?kind=demo&version=v%s&clusterid=%s";
    private static final String REGION = "us-east1";

    private static final HttpClient LICENSE_HTTP_CLIENT = newHttpClient();
    private static final RetryPolicy<HttpResponse<String>> LICENSE_RETRY_POLICY = RetryPolicy.<HttpResponse<String>>builder()
            .handle(IOException.class)
            .withDelay(50, 100, ChronoUnit.MILLIS)
            .withMaxAttempts(100)
            .build();

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
            String clusterId = handle.select("SELECT crdb_internal.cluster_id()").mapTo(String.class).one();
            handle.execute("SET CLUSTER SETTING \"enterprise.license\" = ?", getCockroachLicense(clusterId));
            handle.execute("SET CLUSTER SETTING \"kv.closed_timestamp.lead_for_global_reads_override\" = '1ms'");
            handle.execute(format("ALTER DATABASE %s SET PRIMARY REGION \"%s\"", getDatabaseName(), REGION));
            handle.execute("CREATE DATABASE IF NOT EXISTS %s PRIMARY REGION \"%s\" REGIONS = \"%s\" SURVIVE ZONE FAILURE;"
                    .formatted(getDatabaseName() + "_query_history", REGION, REGION));
        });
    }

    private static String getCockroachLicense(String clusterId)
    {
        URI uri = URI.create(format(COCKROACH_LICENSE_URL, COCKROACH_VERSION, clusterId));
        HttpRequest request = HttpRequest.newBuilder(uri).build();
        HttpResponse<String> response = Failsafe.with(LICENSE_RETRY_POLICY).get(() ->
                LICENSE_HTTP_CLIENT.send(request, BodyHandlers.ofString()));
        if (response.statusCode() != HTTP_OK) {
            throw new RuntimeException("Invalid Cockroach license response: " + response);
        }
        return response.body();
    }
}
