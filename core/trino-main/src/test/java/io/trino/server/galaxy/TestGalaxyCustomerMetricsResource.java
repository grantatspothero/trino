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
package io.trino.server.galaxy;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logging;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.Set;

import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGalaxyCustomerMetricsResource
{
    private TestingTrinoServer server;
    private HttpClient client;

    @BeforeAll
    public void setupServer()
    {
        Logging.initialize();

        client = new JettyHttpClient();
        server = TestingTrinoServer.builder().build();
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        Closer closer = Closer.create();
        closer.register(server);
        closer.register(client);
        closer.close();
        server = null;
        client = null;
    }

    @Test
    public void testGalaxyCustomerMetrics()
    {
        Request request = prepareGet()
                .setHeader(TRINO_HEADERS.requestUser(), "user")
                .setUri(uriBuilderFrom(server.getBaseUrl().resolve("/galaxy/metrics")).build())
                .build();

        Set<String> expectedMetrics = ImmutableSet.<String>builder()
                .add("io_starburst_galaxy_name_GalaxyMetrics_CompletedQueries")
                .add("io_starburst_galaxy_name_GalaxyMetrics_RunningQueries")
                .add("io_starburst_galaxy_name_GalaxyMetrics_QueuedQueries")
                .add("io_starburst_galaxy_name_GalaxyMetrics_FailedQueries")
                .add("io_starburst_galaxy_name_GalaxyMetrics_UserErrorFailures")
                .add("io_starburst_galaxy_name_GalaxyMetrics_InternalFailures")
                .add("io_starburst_galaxy_name_GalaxyMetrics_AbandonedQueries")
                .add("io_starburst_galaxy_name_GalaxyMetrics_CanceledQueries")
                .build();

        assertThat(client.execute(request, createStringResponseHandler()))
                .satisfies(response -> assertThat(response.getStatusCode()).isEqualTo(OK.code()))
                .satisfies(response -> assertThat(Splitter.on("\n").omitEmptyStrings().splitToList(response.getBody()
                        .replaceAll("(?m)^#.*", "")
                        .replaceAll("(?m)^[ \\t]*\\r?\\n", "")
                        .replaceAll("(?m) .*$", "")))
                        .containsExactlyInAnyOrderElementsOf(expectedMetrics));
    }
}
