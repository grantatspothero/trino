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

import io.trino.spi.session.PropertyMetadata;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;

public final class PropertyMetadataValidation
{
    private PropertyMetadataValidation() {}

    public static void verifyPropertyMetadata(PropertyMetadata<?> property, PropertyMetadata<?> existing)
    {
        verify(property.getName().equals(existing.getName()),
                "Mismatched name '%s' <> '%s' for property",
                property.getName(), existing.getName());

        verify(property.getJavaType().equals(existing.getJavaType()),
                "Mismatched Java type '%s' <> '%s' for property: %s",
                property.getJavaType(), existing.getJavaType(), property.getName());

        verify(property.getSqlType().equals(existing.getSqlType()),
                "Mismatched SQL type '%s' <> '%s' for property: %s",
                property.getSqlType(), existing.getSqlType(), property.getName());

        verify(Objects.equals(property.getDefaultValue(), existing.getDefaultValue()),
                "Mismatched default value '%s' <> '%s' for property: %s",
                property.getDefaultValue(), existing.getDefaultValue(), property.getName());
    }

    public static void verifyPropertyDescription(PropertyMetadata<?> property, PropertyMetadata<?> existing)
    {
        verify(property.getDescription().equals(existing.getDescription()),
                "Mismatched description '%s' <> '%s' for property: %s",
                property.getDescription(), existing.getDescription(), property.getName());
    }

    public static void addProperty(Map<String, PropertyMetadata<?>> properties, PropertyMetadata<?> property)
    {
        checkArgument(!properties.containsKey(property.getName()), "Duplicate property: %s", property.getName());
        properties.put(property.getName(), property);
    }
}
