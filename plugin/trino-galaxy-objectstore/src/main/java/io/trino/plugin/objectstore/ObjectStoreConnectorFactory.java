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

import com.google.inject.Binder;
import com.google.inject.Module;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;

public class ObjectStoreConnectorFactory
        implements ConnectorFactory
{
    private final String name;

    public ObjectStoreConnectorFactory(String name)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        ClassLoader classLoader = context.duplicatePluginClassLoader();
        try {
            // the module must be from the same classloader as InternalObjectStoreConnectorFactory
            Object moduleInstance = classLoader.loadClass(EmptyModule.class.getName()).getConstructor().newInstance();
            // use the class instance from InternalObjectStoreConnectorFactory's classloader
            Class<?> moduleClass = classLoader.loadClass(Module.class.getName());
            return (Connector) classLoader.loadClass(InternalObjectStoreConnectorFactory.class.getName())
                    .getMethod("createConnector", String.class, Map.class, Optional.class, Optional.class, Optional.class, moduleClass, ConnectorContext.class)
                    .invoke(null, catalogName, config, Optional.empty(), Optional.empty(), Optional.empty(), moduleInstance, context);
        }
        catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            throwIfUnchecked(targetException);
            throw new RuntimeException(targetException);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getName()
    {
        return name;
    }

    public static class EmptyModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}
    }
}
