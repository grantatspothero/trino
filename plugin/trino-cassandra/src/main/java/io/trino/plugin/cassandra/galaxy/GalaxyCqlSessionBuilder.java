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
package io.trino.plugin.cassandra.galaxy;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;

import static java.util.Objects.requireNonNull;

public class GalaxyCqlSessionBuilder
        extends CqlSessionBuilder
{
    private final GalaxyCassandraTunnelManager galaxyCassandraTunnelManager;

    public GalaxyCqlSessionBuilder(GalaxyCassandraTunnelManager galaxyCassandraTunnelManager)
    {
        this.galaxyCassandraTunnelManager = requireNonNull(galaxyCassandraTunnelManager, "galaxyCassandraTunnelManager is null");
    }

    @Override
    protected DriverContext buildContext(DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments)
    {
        return new GalaxyDriverContext(configLoader, programmaticArguments, galaxyCassandraTunnelManager);
    }
}