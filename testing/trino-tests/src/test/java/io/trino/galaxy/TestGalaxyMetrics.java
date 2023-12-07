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
package io.trino.galaxy;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.core.HttpHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestGalaxyMetrics
        extends AbstractTestQueryFramework
{
    private final String validMetricsToken = "0123456789ABCDEF";
    private HttpClient httpClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        httpClient = new JettyHttpClient();

        closeAfterClass(() -> {
            httpClient.close();
            httpClient = null;
        });

        Session session = testSessionBuilder()
                .build();

        return DistributedQueryRunner.builder(session)
                .addExtraProperty("galaxy.metrics.token", validMetricsToken)
                .setNodeCount(1)
                .build();
    }

    @Test
    public void testGalaxyMetrics()
    {
        DistributedQueryRunner distributedQueryRunner = getDistributedQueryRunner();
        URI baseUrl = distributedQueryRunner.getCoordinator().getBaseUrl();

        // test with no key provided
        Request request = prepareGet().setUri(baseUrl.resolve("/metrics")).build();
        StatusResponseHandler.StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN.code());

        // test with invalid key provided
        request = prepareGet().setHeader(HttpHeaders.AUTHORIZATION, "Bearer thisAintRight").setUri(baseUrl.resolve("/metrics")).build();
        response = httpClient.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN.code());

        // test with invalid key format provided
        request = prepareGet().setHeader(HttpHeaders.AUTHORIZATION, "garbage everywhere should fail").setUri(baseUrl.resolve("/metrics")).build();
        response = httpClient.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN.code());

        // test with correct key provided
        request = prepareGet().setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + validMetricsToken).setUri(baseUrl.resolve("/metrics")).build();
        response = httpClient.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK.code());
    }
}
