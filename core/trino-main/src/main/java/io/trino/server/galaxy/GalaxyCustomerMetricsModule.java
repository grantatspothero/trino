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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import io.trino.server.PrefixObjectNameGeneratorModule.PrefixObjectNameGenerator;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNameGenerator;

import javax.management.MBeanServer;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class GalaxyCustomerMetricsModule
        implements Module
{
    private static final String TRINO_PACKAGE = "io.trino.server.galaxy";
    private static final String CUSTOMER_METRICS_PACKAGE = "io.starburst.galaxy";

    // Classes exported to customer metrics
    private static final Set<Class<?>> CLASSES = ImmutableSet.of(
            GalaxyMetrics.class);

    @Override
    public void configure(Binder binder)
    {
        newOptionalBinder(binder, Key.get(ObjectNameGenerator.class, ForCustomerMetrics.class))
                .setBinding().toInstance(new PrefixObjectNameGenerator(TRINO_PACKAGE, CUSTOMER_METRICS_PACKAGE));
        jaxrsBinder(binder).bind(GalaxyCustomerMetricsResource.class);
        Multibinder<Object> setBinder = newSetBinder(binder, Object.class, ForCustomerMetrics.class);
        CLASSES.forEach(clazz -> setBinder.addBinding().to(clazz));
    }

    @ForCustomerMetrics
    @Singleton
    @Provides
    public MBeanExporter createMBeanExporter(
            MBeanExporter defaultMbeanExporter,
            MBeanServer mBeanServer,
            @ForCustomerMetrics Optional<ObjectNameGenerator> objectNameGenerator,
            @ForCustomerMetrics Set<Object> mbeans)
    {
        MBeanExporter mBeanExporter = new MBeanExporter(mBeanServer, objectNameGenerator);
        mbeans.forEach(mBeanExporter::exportWithGeneratedName);
        checkState(Sets.intersection(defaultMbeanExporter.getManagedClasses().keySet(), mBeanExporter.getManagedClasses().keySet()).isEmpty(), "Duplicate object names exported");
        return mBeanExporter;
    }

    @ForCustomerMetrics
    @Singleton
    @Provides
    public NodeInfo createNodeInfo()
    {
        NodeConfig nodeConfig = new NodeConfig();
        // Not used in customer metrics resource
        nodeConfig.setEnvironment("ignored");
        return new NodeInfo(nodeConfig);
    }
}
