/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.gcs;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.ResponseHandlerUtils;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startable;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static io.airlift.http.client.Request.Builder.preparePut;
import static java.util.Objects.requireNonNull;

public class FakeGcs
        implements Startable
{
    private final GenericContainer<?> container;
    private String fakeGcsExternalUrl;

    public FakeGcs()
    {
        // github: https://github.com/fsouza/fake-gcs-server
        // java example: https://github.com/fsouza/fake-gcs-server/tree/main/examples/java
        this.container = new GenericContainer<>("fsouza/fake-gcs-server")
                .withExposedPorts(4443)
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                        "/bin/fake-gcs-server",
                        "-scheme",
                        "http"));
    }

    private void updateExternalUrlWithContainerUrl()
    {
        String modifyExternalUrlRequesUri = fakeGcsExternalUrl + "/_internal/config";
        String updateExternalUrlJson = "{\"externalUrl\":\"%s\"}".formatted(fakeGcsExternalUrl);

        try (JettyHttpClient httpClient = new JettyHttpClient()) {
            Request request = preparePut()
                    .setUri(URI.create(modifyExternalUrlRequesUri))
                    .setHeader("Content-Type", "application/json")
                    .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(updateExternalUrlJson.getBytes(StandardCharsets.UTF_8)))
                    .build();
            httpClient.execute(request, new ResponseHandler<Response, Exception>() {
                @Override
                public Response handleException(Request request, Exception exception)
                {
                    throw ResponseHandlerUtils.propagate(request, exception);
                }

                @Override
                public Response handle(Request request, Response response)
                {
                    if (response.getStatusCode() != 200) {
                        throw new RuntimeException("failed to update fake-gcs-server with external url: " + response);
                    }
                    return response;
                }
            });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start()
    {
        container.start();

        fakeGcsExternalUrl = "http://" + container.getHost() + ":" + container.getFirstMappedPort();

        updateExternalUrlWithContainerUrl();
    }

    @Override
    public void stop()
    {
        container.stop();
    }

    public Storage getGcsClient()
    {
        return StorageOptions.newBuilder()
                .setHost(getFakeGcsExternalUrl())
                .setProjectId("test-project")
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();
    }

    public String getFakeGcsExternalUrl()
    {
        return requireNonNull(fakeGcsExternalUrl, "You must start() FakeGcs before accessing the external url");
    }
}
