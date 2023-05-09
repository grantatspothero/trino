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
package io.trino.connector;

import io.airlift.log.Logger;
import io.trino.server.PluginClassLoader;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

final class TemporaryMemoryLeakHacks
{
    private static final Logger log = Logger.get(TemporaryMemoryLeakHacks.class);

    private TemporaryMemoryLeakHacks() {}

    // reset various caches/etc. to unload anything stuck in the PluginClassLoader - temporary fix until Plugin-specific classloaders are removed from Trino
    static void clearLeaks(ClassLoader connectorClassLoader)
    {
        if (!(connectorClassLoader instanceof PluginClassLoader pluginClassLoader) || !pluginClassLoader.isDuplicate()) {
            // we only care about duplicated PluginClassLoaders that are held in the system currently, thus causing memory leaks
            return;
        }

        clearPublicSuffixMatcherLoader(connectorClassLoader);
        clearAwsRegionUtils(connectorClassLoader);
        clearJsonCodecs(connectorClassLoader);
        clearHadoopStorageProtos(connectorClassLoader);
        clearConfigurationUtils(connectorClassLoader);
        clearConfigurations(connectorClassLoader);
        clearAwsMetrics(connectorClassLoader);
        clearJackson(connectorClassLoader);
        clearApacheValidator(connectorClassLoader);

        // should be last
        closePluginClassLoader(pluginClassLoader);
    }

    private static void closePluginClassLoader(PluginClassLoader pluginClassLoader)
    {
        try {
            pluginClassLoader.close();
        }
        catch (IOException e) {
            log.warn(e, "Could not close pluginClassLoader");
        }
    }

    private static void clearApacheValidator(ClassLoader connectorClassLoader)
    {
        Object validator = getStaticField(connectorClassLoader, "io.airlift.configuration.ConfigurationFactory", "VALIDATOR");
        Object validatorContext = getField(validator, "validatorContext");

        Object factory = getField(validatorContext, "factory");
        clearMap(factory, "properties");
        clearMap(factory, "unwrappedClassCache");
        callNoArgMethod(factory, "close");

        Object constraintsCache = getField(factory, "constraintsCache");
        clearMap(constraintsCache, "constraintValidatorInfo");
        clearMap(constraintsCache, "validators");

        Object annotationsManager = getField(factory, "annotationsManager");
        clearMap(annotationsManager, "compositions");
        clearMap(annotationsManager, "constraintAttributes");

        Object descriptorManager = getField(factory, "descriptorManager");
        callNoArgMethod(descriptorManager, "clear");
    }

    private static void clearJackson(ClassLoader connectorClassLoader)
    {
        Object instance = getStaticField(connectorClassLoader, "com.fasterxml.jackson.databind.type.TypeFactory", "instance");
        callNoArgMethod(getField(instance, "_typeCache"), "clear");
        setFieldToNull(connectorClassLoader, instance, "com.fasterxml.jackson.databind.type.TypeFactory", "_classLoader");

        Object defaultAnnotationIntrospector = getStaticField(connectorClassLoader, "com.fasterxml.jackson.databind.ObjectMapper", "DEFAULT_ANNOTATION_INTROSPECTOR");
        callNoArgMethod(getField(defaultAnnotationIntrospector, "_annotationsInside"), "clear");
    }

    private static void clearAwsMetrics(ClassLoader connectorClassLoader)
    {
        Method unregisterMBean = getStaticMethod(connectorClassLoader, "com.amazonaws.jmx.MBeans", "unregisterMBean", String.class);
        try {
            if (unregisterMBean != null) {
                unregisterMBean.invoke(null, "com.amazonaws.management:type=AwsSdkMetrics");
            }
        }
        catch (ReflectiveOperationException e) {
            log.warn(e, "Could not invoke unregisterMBean");
        }
    }

    private static void clearConfigurations(ClassLoader connectorClassLoader)
    {
        Object field = getStaticField(connectorClassLoader, "org.apache.hadoop.conf.Configuration", "REGISTRY");
        if (field instanceof Map<?, ?> configurations) {
            configurations.forEach((key, ignore) -> clearConfiguration(connectorClassLoader, key));
            configurations.clear();
        }
    }

    private static void clearConfigurationUtils(ClassLoader connectorClassLoader)
    {
        Object configuration = getStaticField(connectorClassLoader, "io.trino.hdfs.ConfigurationUtils", "INITIAL_CONFIGURATION");

        clearConfiguration(connectorClassLoader, configuration);
    }

    private static void clearConfiguration(ClassLoader connectorClassLoader, Object configuration)
    {
        clearCollection(configuration, "resources");
        clearCollection(configuration, "finalParameters");
        clearMap(configuration, "propertyTagsMap");
        clearMap(configuration, "updatingResource");
        clearMap(configuration, "properties");
        clearMap(configuration, "overlay");
        setFieldToNull(connectorClassLoader, configuration, "org.apache.hadoop.conf.Configuration", "classLoader");
    }

    private static void clearHadoopStorageProtos(ClassLoader connectorClassLoader)
    {
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.storage.v2.StorageProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.iam.v1.IamPolicyProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.iam.v1.OptionsProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.iam.v1.PolicyProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.api.AnnotationsProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.api.ClientProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.api.FieldBehaviorProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.api.ResourceProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.protobuf.EmptyProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.protobuf.FieldMaskProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.protobuf.TimestampProto", "descriptor");
        setStaticFieldToNull(connectorClassLoader, "com.google.cloud.hadoop.repackaged.gcs.com.google.type.DateProto", "descriptor");
    }

    private static void clearJsonCodecs(ClassLoader connectorClassLoader)
    {
        Object objectMapperSupplierLambda = getStaticField(connectorClassLoader, "io.airlift.json.JsonCodec", "OBJECT_MAPPER_SUPPLIER");

        if (objectMapperSupplierLambda != null) {
            // the OBJECT_MAPPER_SUPPLIER will have captured the real instance of NonSerializableMemoizingSupplier that we need to null (its value)
            Object objectMapperSupplier = getField(objectMapperSupplierLambda, "arg$1");
            setFieldToNull(connectorClassLoader, objectMapperSupplier, "com.google.common.base.Suppliers$NonSerializableMemoizingSupplier", "value");
            setFieldToNull(connectorClassLoader, objectMapperSupplier, "com.google.common.base.Suppliers$NonSerializableMemoizingSupplier", "delegate");
        }
    }

    private static void clearPublicSuffixMatcherLoader(ClassLoader connectorClassLoader)
    {
        Object publicSuffixMatcher = getStaticField(connectorClassLoader, "org.apache.http.conn.util.PublicSuffixMatcherLoader", "DEFAULT_INSTANCE");

        clearMap(publicSuffixMatcher, "rules");
        clearMap(publicSuffixMatcher, "exceptions");
    }

    private static void clearAwsRegionUtils(ClassLoader connectorClassLoader)
    {
        Object regionMetadata = getStaticField(connectorClassLoader, "com.amazonaws.regions.RegionUtils", "regionMetadata");
        Object provider = getField(regionMetadata, "provider");

        // something is holding on to these even though we're going to null the reference below - so empty/clear them
        clearMap(provider, "partitionMap");
        clearMap(provider, "credentialScopeRegionByHost");
        clearCollection(provider, "standardHostnamePatternDnsSuffixes");
        clearMap(provider, "regionCache");

        setStaticFieldToNull(connectorClassLoader, "com.amazonaws.regions.RegionUtils", "regionMetadata");
    }

    private static Method getStaticMethod(ClassLoader connectorClassLoader, String className, String methodName, Class<?> parameterTypes)
    {
        try {
            Method method = connectorClassLoader.loadClass(className).getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method;
        }
        catch (ReflectiveOperationException e) {
            log.warn(e, "Could not getStaticMethod %s", className + "." + methodName);
        }
        return null;
    }

    private static Object getField(Object instance, String fieldName)
    {
        if (instance != null) {
            try {
                Field field = instance.getClass().getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(instance);
            }
            catch (ReflectiveOperationException e) {
                log.warn(e, "Could not getField %s", instance.getClass().getName() + "." + fieldName);
            }
        }
        return null;
    }

    private static Object getStaticField(ClassLoader connectorClassLoader, String className, String fieldName)
    {
        try {
            Field field = connectorClassLoader.loadClass(className).getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(null);
        }
        catch (ReflectiveOperationException e) {
            log.warn(e, "Could not getStaticField %s", className + "." + fieldName);
        }
        return null;
    }

    private static void setFieldToNull(ClassLoader connectorClassLoader, Object instance, String className, String fieldName)
    {
        if (instance == null) {
            return;
        }

        try {
            Field field = connectorClassLoader.loadClass(className).getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(instance, null);
        }
        catch (ReflectiveOperationException e) {
            log.warn(e, "Could not setFieldToNull %s", className + "." + fieldName);
        }
    }

    private static void setStaticFieldToNull(ClassLoader connectorClassLoader, String className, String fieldName)
    {
        try {
            Field field = connectorClassLoader.loadClass(className).getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(null, null);
        }
        catch (ReflectiveOperationException e) {
            log.warn(e, "Could not setStaticFieldToNull %s", className + "." + fieldName);
        }
    }

    private static void clearMap(Object instance, String name)
    {
        Object field = getField(instance, name);
        if (field instanceof Map<?, ?> map) {
            map.clear();
        }
    }

    private static void clearCollection(Object instance, String name)
    {
        Object field = getField(instance, name);
        if (field instanceof Collection<?> collection) {
            collection.clear();
        }
    }

    private static void callNoArgMethod(Object instance, String methodName)
    {
        if (instance != null) {
            try {
                Method method = instance.getClass().getDeclaredMethod(methodName);
                method.setAccessible(true);
                method.invoke(instance);
            }
            catch (ReflectiveOperationException e) {
                log.warn(e, "Could not callNoArgMethod %s", methodName);
            }
        }
    }
}
