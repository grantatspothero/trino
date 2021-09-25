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
package io.trino.hdfs.galaxy;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.hdfs.ConfigurationInitializer;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class RegionEnforcementModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        newSetBinder(binder, ConfigurationInitializer.class).addBinding()
                .to(RegionEnforcementConfigurationInitializer.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(RegionEnforcementConfig.class);
    }
}
