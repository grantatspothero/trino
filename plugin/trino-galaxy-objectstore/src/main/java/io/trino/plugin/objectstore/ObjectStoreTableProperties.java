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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import io.trino.spi.connector.Connector;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.objectstore.PropertyMetadataValidation.addProperty;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.verifyPropertyDescription;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.verifyPropertyMetadata;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class ObjectStoreTableProperties
{
    private final ImmutableMap<String, PropertyMetadata<?>> properties;
    private final SetMultimap<String, TableType> tableTypesForProperty;

    @Inject
    public ObjectStoreTableProperties(
            @ForHive Connector hiveConnector,
            @ForIceberg Connector icebergConnector,
            @ForDelta Connector deltaConnector,
            @ForHudi Connector hudiConnector,
            ObjectStoreConfig config)
    {
        Map<String, PropertyMetadata<?>> properties = new HashMap<>();
        ImmutableSet<String> ignoredDescriptions = ImmutableSet.<String>builder()
                .add("location")
                .add("partitioned_by")
                .add("orc_bloom_filter_columns")
                .build();

        ImmutableSetMultimap.Builder<String, TableType> tableTypesForProperty = ImmutableSetMultimap.builder();

        for (PropertyMetadata<?> property : hiveConnector.getTableProperties()) {
            if (property.getName().equals("format")) {
                continue;
            }
            if (property.getName().equals("sorted_by")) {
                continue;
            }
            addProperty(properties, property);
            tableTypesForProperty.put(property.getName(), HIVE);
        }

        for (PropertyMetadata<?> property : icebergConnector.getTableProperties()) {
            if (property.getName().equals("format")) {
                continue;
            }
            if (property.getName().equals("sorted_by")) {
                continue;
            }
            PropertyMetadata<?> existing = properties.putIfAbsent(property.getName(), property);
            if (existing != null) {
                verifyPropertyMetadata(property, existing);
                if (!ignoredDescriptions.contains(property.getName())) {
                    verifyPropertyDescription(property, existing);
                }
            }
            tableTypesForProperty.put(property.getName(), ICEBERG);
        }

        for (PropertyMetadata<?> property : deltaConnector.getTableProperties()) {
            if (property.getName().equals("format")) {
                throw new VerifyException("Unexpected 'format' property for Delta Lake");
            }
            PropertyMetadata<?> existing = properties.putIfAbsent(property.getName(), property);
            if (existing != null) {
                verifyPropertyMetadata(property, existing);
                if (!ignoredDescriptions.contains(property.getName())) {
                    verifyPropertyDescription(property, existing);
                }
            }
            tableTypesForProperty.put(property.getName(), DELTA);
        }

        for (PropertyMetadata<?> property : hudiConnector.getTableProperties()) {
            if (property.getName().equals("format")) {
                throw new VerifyException("Unexpected 'format' property for Hudi");
            }
            PropertyMetadata<?> existing = properties.putIfAbsent(property.getName(), property);
            if (existing != null) {
                verifyPropertyMetadata(property, existing);
                if (!ignoredDescriptions.contains(property.getName())) {
                    verifyPropertyDescription(property, existing);
                }
            }
            tableTypesForProperty.put(property.getName(), HUDI);
        }

        addProperty(properties, new PropertyMetadata<>(
                "sorted_by",
                "Sorted columns",
                new ArrayType(VARCHAR),
                List.class,
                ImmutableList.of(),
                false,
                value -> (List<?>) value,
                value -> value));

        addProperty(properties, stringProperty(
                "format",
                "File format for the table",
                "ORC",
                false));

        addProperty(properties, enumProperty(
                "type",
                "Table format",
                TableType.class,
                config.getTableType(),
                false));

        this.properties = ImmutableMap.copyOf(properties);
        this.tableTypesForProperty = tableTypesForProperty.build();
    }

    public List<PropertyMetadata<?>> getProperties()
    {
        return properties.values().asList();
    }

    public boolean validProperty(TableType tableType, String name, Object value)
    {
        if (name.equals("type")) {
            return false;
        }
        if (name.equals("format")) {
            return (tableType == HIVE) || (tableType == ICEBERG);
        }
        if (name.equals("sorted_by")) {
            return (tableType == HIVE) || (tableType == ICEBERG);
        }
        if (tableTypesForProperty.containsEntry(name, tableType)) {
            return true;
        }
        PropertyMetadata<?> property = properties.get(name);
        if ((property == null) || !value.equals(property.getDefaultValue())) {
            throw new VerifyException("Table property '%s' not supported for %s tables".formatted(name, tableType.displayName()));
        }
        return false;
    }
}
