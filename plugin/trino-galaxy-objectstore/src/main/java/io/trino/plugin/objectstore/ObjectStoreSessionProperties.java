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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.Connector;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.IGNORE_DESCRIPTION;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.VERIFY_DESCRIPTION;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.verifyPropertyMetadata;

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
        this.sessionProperties = ImmutableList.copyOf(sessionProperties.values());
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
