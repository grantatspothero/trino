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

import com.google.common.base.Throwables;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;
import java.util.stream.IntStream;

import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public final class Procedures
{
    private Procedures() {}

    public static Procedure unwrapSession(ObjectStoreSessionProperties sessionProperties, TableType type, Procedure procedure)
    {
        MethodHandle methodHandle = procedure.getMethodHandle();
        int parameterCount = methodHandle.type().parameterList().size();
        int[] sessionPositions = IntStream.range(0, parameterCount)
                .filter(i -> methodHandle.type().parameterType(i) == ConnectorSession.class)
                .toArray();

        if (sessionPositions.length == 0) {
            return procedure;
        }

        MethodHandle spreader = methodHandle.asSpreader(Object[].class, parameterCount);
        UnwrapSession proxy = new UnwrapSession(sessionProperties, type, sessionPositions, spreader);
        MethodHandle proxyMethodHandle = UnwrapSession.INVOKE.bindTo(proxy)
                .asCollector(Object[].class, parameterCount)
                .asType(methodHandle.type());

        return new Procedure(
                procedure.getSchema(),
                procedure.getName(),
                procedure.getArguments(),
                proxyMethodHandle,
                procedure.requiresNamedArguments());
    }

    static class UnwrapSession
    {
        private static final MethodHandle INVOKE;

        static {
            try {
                INVOKE = lookup().unreflect(UnwrapSession.class.getMethod("invoke", Object[].class));
            }
            catch (ReflectiveOperationException e) {
                throw new LinkageError(e.getMessage(), e);
            }
        }

        private final ObjectStoreSessionProperties sessionProperties;
        private final TableType type;
        private final int[] sessionPositions;
        private final MethodHandle delegate;

        public UnwrapSession(ObjectStoreSessionProperties sessionProperties, TableType type, int[] sessionPositions, MethodHandle delegate)
        {
            this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
            this.type = requireNonNull(type, "type is null");
            this.sessionPositions = sessionPositions.clone();
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        public Object invoke(Object[] arguments)
        {
            Object[] copy = arguments.clone();
            ConnectorSession unwrapped = null;
            for (int position : sessionPositions) {
                if (unwrapped == null) {
                    ConnectorSession session = (ConnectorSession) arguments[position];
                    unwrapped = sessionProperties.unwrap(type, session);
                }
                copy[position] = unwrapped;
            }
            try {
                return delegate.invoke(copy);
            }
            catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }
    }
}
