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
package io.trino.server.security.galaxy;

import com.google.inject.Inject;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.server.galaxy.GalaxyAuthorizationClientModule;
import io.trino.server.galaxy.GalaxyPermissionsCache;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;

import java.util.Map;

import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class GalaxyTrinoSystemAccessFactory
        implements SystemAccessControlFactory
{
    public static final String NAME = "galaxy";

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;

    @Inject
    public GalaxyTrinoSystemAccessFactory(OpenTelemetry openTelemetry)
    {
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.tracer = openTelemetry.getTracer("trino.system-access-control." + NAME);
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public SystemAccessControl create(Map<String, String> properties)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new GalaxyAuthorizationClientModule(),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(openTelemetry);
                    binder.bind(Tracer.class).toInstance(tracer);
                    binder.bind(GalaxyPermissionsCache.class).in(SINGLETON);
                    binder.bind(GalaxySystemAccessController.class).in(SINGLETON);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(properties)
                .initialize();

        GalaxySystemAccessController controller = injector.getInstance(GalaxySystemAccessController.class);
        return new GalaxyAccessControl(ignore -> controller);
    }
}
