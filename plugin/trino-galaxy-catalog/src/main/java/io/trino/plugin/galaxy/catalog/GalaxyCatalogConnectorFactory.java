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
package io.trino.plugin.galaxy.catalog;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class GalaxyCatalogConnectorFactory
        implements ConnectorFactory
{
    public static final String CONNECTOR_NAME = "galaxy_catalog";

    private final Module additionalModule;

    public GalaxyCatalogConnectorFactory()
    {
        this(new GalaxyCatalogFunctionApiModule());
    }

    GalaxyCatalogConnectorFactory(Module additionalModule)
    {
        this.additionalModule = requireNonNull(additionalModule, "additionalModule is null");
    }

    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new GalaxyCatalogModule(),
                additionalModule);

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(GalaxyCatalogConnector.class);
    }
}
