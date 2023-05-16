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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TimeZoneKey;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDefaultValue.IGNORE_DEFAULT_VALUE;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDefaultValue.VERIFY_DEFAULT_VALUE;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.IGNORE_DESCRIPTION;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.VERIFY_DESCRIPTION;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.verifyPropertyMetadata;
import static java.util.Objects.requireNonNull;

public class ObjectStoreSessionProperties
{
    private final List<PropertyMetadata<?>> sessionProperties;
    private final Table<String, TableType, Optional<Object>> defaultPropertyValue;

    @Inject
    public ObjectStoreSessionProperties(DelegateConnectors delegates)
    {
        Set<String> ignoredDescriptions = ImmutableSet.<String>builder()
                .add("compression_codec")
                .add("projection_pushdown_enabled")
                .add("timestamp_precision")
                .add("minimum_assigned_split_weight")
                .build();

        Table<String, TableType, PropertyMetadata<?>> delegateProperties = HashBasedTable.create();
        delegates.byType().forEach((type, connector) -> {
            for (PropertyMetadata<?> property : connector.getSessionProperties()) {
                delegateProperties.put(property.getName(), type, property);
            }
        });

        ImmutableList.Builder<PropertyMetadata<?>> sessionProperties = ImmutableList.builder();
        ImmutableTable.Builder<String, TableType, Optional<Object>> defaultPropertyValue = ImmutableTable.builder();
        for (Map.Entry<String, Map<TableType, PropertyMetadata<?>>> propertyCandidatesEntry : delegateProperties.rowMap().entrySet()) {
            String name = propertyCandidatesEntry.getKey();
            Map<TableType, PropertyMetadata<?>> propertiesOfName = propertyCandidatesEntry.getValue();
            PropertyMetadataValidation.VerifyDescription verifyDescription = ignoredDescriptions.contains(name) ? IGNORE_DESCRIPTION : VERIFY_DESCRIPTION;
            PropertyMetadata<?> first = Stream.of(TableType.values())
                    .map(propertiesOfName::get)
                    .filter(Objects::nonNull)
                    .findFirst().orElseThrow();

            boolean commonDefaultValue = propertiesOfName.values().stream()
                    .map(PropertyMetadata::getDefaultValue)
                    .distinct()
                    .count() == 1;

            if (commonDefaultValue) {
                for (PropertyMetadata<?> existing : propertiesOfName.values()) {
                    verifyPropertyMetadata(
                            first,
                            existing,
                            VERIFY_DEFAULT_VALUE, // technically redundant now
                            verifyDescription);
                }
                sessionProperties.add(first);
            }
            else {
                for (Map.Entry<TableType, PropertyMetadata<?>> entry : propertiesOfName.entrySet()) {
                    verifyPropertyMetadata(
                            first,
                            entry.getValue(),
                            IGNORE_DEFAULT_VALUE,
                            verifyDescription);
                    defaultPropertyValue.put(name, entry.getKey(), Optional.ofNullable(entry.getValue().getDefaultValue()));
                }
                sessionProperties.add(first.withDefault(null));
            }
        }

        this.sessionProperties = sessionProperties.build().stream()
                .map(ObjectStoreSessionProperties::toWrapped)
                .collect(toImmutableList());
        this.defaultPropertyValue = defaultPropertyValue.buildOrThrow();
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

    private class DelegateSession
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
            Object connectorValue = wrappedPropertyValue.value();
            if (connectorValue == null) {
                @Nullable
                Optional<Object> defaultValue = defaultPropertyValue.get(name, forType);
                //noinspection OptionalAssignedToNull
                if (defaultValue != null) {
                    connectorValue = defaultValue.orElse(null);
                }
            }
            return type.cast(connectorValue);
        }
    }
}
