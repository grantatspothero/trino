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
package io.trino.plugin.base.jmx;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;

import javax.inject.Singleton;
import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class MBeanServerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(MBeanServerModuleConfig.class);
    }

    @Provides
    @Singleton
    public MBeanServer getMBeanServer(MBeanServerModuleConfig config)
    {
        MBeanServer mBeanServer = config.getPlatformMBeanServerEnabled() ? ManagementFactory.getPlatformMBeanServer() : new NopMBeanServer();
        return new RebindSafeMBeanServer(mBeanServer);
    }
}
