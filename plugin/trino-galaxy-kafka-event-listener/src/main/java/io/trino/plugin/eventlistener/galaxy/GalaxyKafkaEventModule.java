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
package io.trino.plugin.eventlistener.galaxy;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.galaxy.kafka.KafkaPublisherConfig;
import io.trino.plugin.eventlistener.galaxy.event.GalaxyQueryCompletedEvent;
import io.trino.plugin.eventlistener.galaxy.event.GalaxyQueryLifeCycleEvent;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class GalaxyKafkaEventModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(KafkaPublisherConfig.class);
        configBinder(binder).bindConfig(GalaxyKafkaEventListenerConfig.class);
        jsonCodecBinder(binder).bindJsonCodec(GalaxyQueryCompletedEvent.class);
        jsonCodecBinder(binder).bindJsonCodec(GalaxyQueryLifeCycleEvent.class);
        binder.bind(GalaxyKafkaEventListener.class).in(Scopes.SINGLETON);
        String pluginReportingName = buildConfigObject(GalaxyKafkaEventListenerConfig.class).getPluginReportingName();
        newExporter(binder).export(GalaxyKafkaEventListener.class)
                .as(generator -> generator.generatedNameOf(GalaxyKafkaEventListener.class, pluginReportingName));
    }
}