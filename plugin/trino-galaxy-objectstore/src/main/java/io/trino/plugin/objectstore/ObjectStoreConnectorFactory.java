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

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.objectstore.InternalObjectStoreConnectorFactory.createConnector;

public class ObjectStoreConnectorFactory
        implements ConnectorFactory
{
    private static final String GALAXY_OBJECTSTORE = "galaxy_objectstore";

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(
                catalogName,
                config,
                Optional.empty(),
                Optional.empty(),
                EMPTY_MODULE,
                Optional.empty(),
                Optional.empty(),
                EMPTY_MODULE,
                context);
    }

    @Override
    public String getName()
    {
        return GALAXY_OBJECTSTORE;
    }
}
