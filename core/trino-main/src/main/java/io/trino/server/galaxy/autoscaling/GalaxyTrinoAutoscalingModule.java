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
package io.trino.server.galaxy.autoscaling;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.server.galaxy.autoscaling.GalaxyTrinoAutoscalingConfig.AutoscalingMethod.TIME_OPTIMAL;
import static io.trino.server.galaxy.autoscaling.GalaxyTrinoAutoscalingConfig.AutoscalingMethod.TIME_RATIO;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class GalaxyTrinoAutoscalingModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GalaxyTrinoAutoscalingConfig.class);
        binder.bind(TrinoAutoscalingStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TrinoAutoscalingStats.class).withGeneratedName();

        install(conditionalModule(
                GalaxyTrinoAutoscalingConfig.class,
                config -> config.getAutoscalingMethod() == TIME_RATIO, new QueryTimeRatioBasedEstimatorModule()));
        install(conditionalModule(
                GalaxyTrinoAutoscalingConfig.class,
                config -> config.getAutoscalingMethod() == TIME_OPTIMAL, new QueryTimeBasedOptimalEstimatorModule()));
    }
}
