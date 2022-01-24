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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.starburst.stargate.accesscontrol.client.TrinoLocationApi;
import io.starburst.stargate.identity.DispatchSession;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.io.Closeable;
import java.util.Map;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

public class TestingLocationSecurityServer
        implements Closeable
{
    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;

    public TestingLocationSecurityServer(TrinoLocationApi trinoLocationApi)
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                new JaxrsModule(),
                new JsonModule(),
                binder -> {
                    binder.bind(TrinoLocationApi.class).toInstance(trinoLocationApi);
                    jaxrsBinder(binder).bind(TrinoLocationResource.class);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        server = injector.getInstance(TestingHttpServer.class);
    }

    public Map<String, String> getClientConfig()
    {
        return ImmutableMap.<String, String>builder()
                .put("galaxy.account-url", server.getBaseUrl().toString())
                .buildOrThrow();
    }

    @Override
    public void close()
    {
        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
    }

    @Path("api/v1/galaxy/security/trino/location")
    public static class TrinoLocationResource
    {
        private final TrinoLocationApi locationApi;

        @Inject
        public TrinoLocationResource(TrinoLocationApi locationApi)
        {
            this.locationApi = requireNonNull(locationApi, "locationApi is null");
        }

        @POST
        @Consumes(TEXT_PLAIN)
        public Response canUseLocation(@Context DispatchSession session, String location)
        {
            if (locationApi.canUseLocation(session, location)) {
                return Response.ok().build();
            }
            return Response.status(NOT_FOUND).build();
        }
    }
}
