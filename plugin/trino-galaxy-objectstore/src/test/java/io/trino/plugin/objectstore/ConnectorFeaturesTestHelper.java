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

import io.trino.testing.BaseConnectorTest;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.fail;

class ConnectorFeaturesTestHelper
{
    private final Class<?> objectStoreConnectorFeaturesTestClass;
    private final Class<?> connectorTestClass;
    private final Class<?> objectStoreTestClass;

    public ConnectorFeaturesTestHelper(Class<? extends BaseConnectorTest> objectStoreConnectorFeaturesTestClass, Class<? extends BaseObjectStoreConnectorTest> objectStoreTestClass)
    {
        checkArgument(!BaseObjectStoreConnectorTest.class.isAssignableFrom(objectStoreConnectorFeaturesTestClass));
        this.objectStoreConnectorFeaturesTestClass = requireNonNull(objectStoreConnectorFeaturesTestClass, "objectStoreTestClass is null");
        this.connectorTestClass = objectStoreConnectorFeaturesTestClass.getSuperclass();
        checkArgument(BaseConnectorTest.class.isAssignableFrom(connectorTestClass.getSuperclass()));
        this.objectStoreTestClass = requireNonNull(objectStoreTestClass, "objectStoreTestClass is null");
    }

    void preventDuplicatedTestCoverage(Method testMethod)
    {
        Class<?> declaringClass = testMethod.getDeclaringClass();
        if (declaringClass.isAssignableFrom(BaseConnectorTest.class) && !isOverridden(testMethod, objectStoreConnectorFeaturesTestClass)) {
            fail("The %s test is covered by %s, no need to run it again in %s. You can use main() to generate overrides".formatted(
                    testMethod.getName(),
                    objectStoreTestClass .getSimpleName(),
                    objectStoreConnectorFeaturesTestClass.getSimpleName()));
        }
    }

    void skipDuplicateTestCoverage(String methodName, Class<?>... args)
    {
        try {
            Method ignored = objectStoreConnectorFeaturesTestClass.getDeclaredMethod(methodName, args); // validate we have the override
            if (isTestSpecializedForConnector(methodName, args)) {
                fail("Method %s(%s) became overridden and should no longer be skipped in %s".formatted(methodName, Arrays.toString(args), objectStoreConnectorFeaturesTestClass));
            }
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }

        throw new SkipException("This method is probably run in %s".formatted(objectStoreTestClass.getSimpleName()));
    }

    private boolean isTestSpecializedForConnector(String methodName, Class<?>... args)
    {
        try {
            Method overridden = connectorTestClass.getMethod(methodName, args);
            return !overridden.getDeclaringClass().isAssignableFrom(BaseConnectorTest.class);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public void generateOverrides()
    {
        Stream.of(BaseConnectorTest.class.getMethods())
                .filter(method -> method.isAnnotationPresent(Test.class) && !isOverridden(method, objectStoreConnectorFeaturesTestClass))
                .sorted(Comparator.comparing(Method::getName))
                .forEachOrdered(method -> {
                    System.out.printf(
                            """
                                    @Override
                                    public void %s(%s)
                                    {
                                        skipDuplicateTestCoverage("%1$s"%s);
                                    }
                                    \n""", method.getName(),
                            IntStream.range(0, method.getParameterTypes().length)
                                    .mapToObj(i -> "%s arg%s".formatted(formatClassName(method.getParameterTypes()[i]), i))
                                    .collect(joining(", ")),
                            Stream.of(method.getParameterTypes())
                                    .map(clazz -> ", %s.class".formatted(formatClassName(clazz)))
                                    .collect(joining()));
                });
    }

    private static boolean isOverridden(Method method, Class<?> byClazz)
    {
        checkArgument(
                method.getDeclaringClass() != byClazz && method.getDeclaringClass().isAssignableFrom(byClazz),
                "%s is not a subclass of %s which declares %s",
                byClazz,
                method.getDeclaringClass(),
                method);

        for (Class<?> clazz = byClazz; clazz != method.getDeclaringClass(); clazz = clazz.getSuperclass()) {
            try {
                Method ignored = clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
                return true;
            }
            catch (NoSuchMethodException ignore) {
                // continue
            }
        }
        return false;
    }

    private static String formatClassName(Class<?> clazz)
    {
        String className = clazz.getSimpleName();
        // IntelliJ does not auto-import nested names, so use the top-level class name
        for (Class<?> enclosingClass = clazz.getEnclosingClass(); enclosingClass != null; enclosingClass = enclosingClass.getEnclosingClass()) {
            //noinspection StringConcatenationInLoop
            className = enclosingClass.getSimpleName() + "." + className;
        }
        return className;
    }
}
