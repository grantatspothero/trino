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
package io.trino.server.testing;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.tracing.SpanSerialization;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;

import static io.airlift.json.JsonBinder.jsonBinder;
import static java.util.Objects.requireNonNull;

// TODO use airlift's TestingTracingModule once https://github.com/airlift/airlift/pull/1090 is available
public class TestingTracingModule
        extends AbstractConfigurationAwareModule
{
    private final String serviceName;
    private final SpanProcessor spanProcessor;

    public TestingTracingModule(String serviceName, SpanProcessor spanProcessor)
    {
        this.serviceName = requireNonNull(serviceName, "serviceName is null");
        this.spanProcessor = requireNonNull(spanProcessor, "spanProcessor is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        jsonBinder(binder).addSerializerBinding(Span.class).to(SpanSerialization.SpanSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Span.class).to(SpanSerialization.SpanDeserializer.class);
    }

    @Provides
    @Singleton
    public OpenTelemetry createOpenTelemetry()
    {
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor)
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    @Provides
    public Tracer createTracer(OpenTelemetry openTelemetry)
    {
        return openTelemetry.getTracer(serviceName);
    }
}
