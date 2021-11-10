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

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.galaxy.kafka.KafkaPublisherConfig;
import io.trino.server.galaxy.events.HeartbeatPublisher;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class GalaxyHeartbeatModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GalaxyConfig.class);
        configBinder(binder).bindConfig(GalaxyHeartbeatConfig.class);
        configBinder(binder).bindConfig(KafkaPublisherConfig.class);
        binder.bind(HeartbeatPublisher.class).in(SINGLETON);
        newExporter(binder).export(HeartbeatPublisher.class).withGeneratedName();
    }
}
