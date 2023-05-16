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

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TimeZoneKey;

import javax.inject.Inject;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.IGNORE_DESCRIPTION;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.VERIFY_DESCRIPTION;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.verifyPropertyMetadata;
import static java.util.Objects.requireNonNull;

public class ObjectStoreSessionProperties
{
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ObjectStoreSessionProperties(DelegateConnectors delegates)
    {
        Set<String> ignoredDescriptions = ImmutableSet.<String>builder()
                .add("compression_codec")
                .add("projection_pushdown_enabled")
                .add("timestamp_precision")
                .add("minimum_assigned_split_weight")
                .build();

        Map<String, PropertyMetadata<?>> sessionProperties = new HashMap<>();
        for (Connector connector : delegates.asList()) {
            for (PropertyMetadata<?> property : connector.getSessionProperties()) {
                PropertyMetadata<?> existing = sessionProperties.putIfAbsent(property.getName(), property);
                if (existing != null) {
                    verifyPropertyMetadata(
                            property,
                            existing,
                            ignoredDescriptions.contains(property.getName()) ? IGNORE_DESCRIPTION : VERIFY_DESCRIPTION);
                }
            }
        }
        this.sessionProperties = sessionProperties.values().stream()
                .map(ObjectStoreSessionProperties::toWrapped)
                .collect(toImmutableList());
    }

    public ConnectorSession unwrap(TableType forType, ConnectorSession session)
    {
        return new DelegateSession(forType, session);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    private static <T> PropertyMetadata<WrappedPropertyValue> toWrapped(PropertyMetadata<T> propertyMetadata)
    {
        // The WrappedValue is a wrapper type making sure we unwrap ConnectorSession properties explicitly
        // when leaving ObjectStore connector code into delegate connector OR we fail loudly.
        return propertyMetadata.convert(
                WrappedPropertyValue.class,
                WrappedPropertyValue::new,
                wrappedPropertyValue -> propertyMetadata.getJavaType().cast(wrappedPropertyValue.value()));
    }

    private static class DelegateSession
            implements ConnectorSession
    {
        private final TableType forType;
        private final ConnectorSession baseSession; // from engine

        public DelegateSession(TableType forType, ConnectorSession baseSession)
        {
            this.forType = requireNonNull(forType, "forType is null");
            this.baseSession = requireNonNull(baseSession, "baseSession is null");
        }

        @Override
        public String getQueryId()
        {
            return baseSession.getQueryId();
        }

        @Override
        public Optional<String> getSource()
        {
            return baseSession.getSource();
        }

        @Override
        public String getUser()
        {
            return baseSession.getUser();
        }

        @Override
        public ConnectorIdentity getIdentity()
        {
            return baseSession.getIdentity();
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return baseSession.getTimeZoneKey();
        }

        @Override
        public Locale getLocale()
        {
            return baseSession.getLocale();
        }

        @Override
        public Optional<String> getTraceToken()
        {
            return baseSession.getTraceToken();
        }

        @Override
        public Instant getStart()
        {
            return baseSession.getStart();
        }

        @Override
        public <T> T getProperty(String name, Class<T> type)
        {
            WrappedPropertyValue wrappedPropertyValue = baseSession.getProperty(name, WrappedPropertyValue.class);
            return type.cast(wrappedPropertyValue.value());
        }
    }
}
