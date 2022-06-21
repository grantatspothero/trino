/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.metadata.server;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.starburst.stargate.buffer.metadata.database.DatabaseModule;
import io.starburst.stargate.buffer.metadata.model.Chunk;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.MySQLContainer;

import java.net.URI;
import java.util.Map;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Closeables.closeAll;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMetadataServer
{
    private static final JsonCodec<Chunk> CHUNK_CODEC = jsonCodec(Chunk.class);

    private MySQLContainer<?> mysql;
    private HttpClient client;
    private TestingHttpServer server;
    private LifeCycleManager lifeCycleManager;

    @BeforeAll
    public void setup()
    {
        mysql = new MySQLContainer<>("mysql:8.0");
        mysql.start();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("db.url", mysql.getJdbcUrl())
                .put("db.user", mysql.getUsername())
                .put("db.password", mysql.getPassword())
                .build();

        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new DatabaseModule(),
                new MainModule());

        Injector injector = app
                .setRequiredConfigurationProperties(properties)
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        server = injector.getInstance(TestingHttpServer.class);

        client = new JettyHttpClient();

        setupDatabase(injector.getInstance(Jdbi.class));
    }

    @AfterAll
    public void teardown()
            throws Exception
    {
        closeAll(lifeCycleManager::stop, client, mysql);
    }

    @Test
    public void testGetChunkFound()
    {
        JsonResponse<Chunk> response = client.execute(
                prepareGet().setUri(uriFor("/api/v1/metadata/chunk/123")).build(),
                createFullJsonResponseHandler(CHUNK_CODEC));

        assertThat(response.hasValue()).isTrue();
        assertThat(response.getValue()).isEqualTo(new Chunk(123));
    }

    @Test
    public void testGetChunkNotFound()
    {
        JsonResponse<Chunk> response = client.execute(
                prepareGet().setUri(uriFor("/api/v1/metadata/chunk/999")).build(),
                createFullJsonResponseHandler(CHUNK_CODEC));

        assertThat(response.getStatusCode()).isEqualTo(HTTP_NOT_FOUND);
        assertThat(response.hasValue()).isFalse();
    }

    private URI uriFor(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

    private static void setupDatabase(Jdbi jdbi)
    {
        jdbi.useHandle(handle -> {
            handle.execute("CREATE TABLE chunks (chunk_id INT PRIMARY KEY)");
            handle.execute("INSERT INTO chunks (chunk_id) VALUES (?)", 123);
        });
    }
}
