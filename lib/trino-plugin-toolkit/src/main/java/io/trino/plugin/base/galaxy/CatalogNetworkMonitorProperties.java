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
package io.trino.plugin.base.galaxy;

import io.airlift.units.DataSize;
import io.trino.spi.connector.CatalogHandle;

import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public record CatalogNetworkMonitorProperties(
        String catalogName,
        String catalogId,
        boolean tlsEnabled,
        boolean crossRegionAllowed,
        Optional<DataSize> crossRegionReadLimit,
        Optional<DataSize> crossRegionWriteLimit)
{
    public static final String CATALOG_NAME_PROPERTY_NAME = "catalogName";
    public static final String CATALOG_ID_PROPERTY_NAME = "catalogId";
    public static final String TLS_ENABLED_PROPERTY_NAME = "tlsEnabled";
    public static final String CROSS_REGION_ALLOWED_PROPERTY_NAME = "crossRegionAllowed";
    private static final String CROSS_REGION_READ_LIMIT_PROPERTY_NAME = "crossRegionReadLimit";
    private static final String CROSS_REGION_WRITE_LIMIT_PROPERTY_NAME = "crossRegionWriteLimit";

    public CatalogNetworkMonitorProperties
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(catalogId, "catalogId is null");
        requireNonNull(crossRegionReadLimit, "crossRegionReadLimit is null");
        requireNonNull(crossRegionWriteLimit, "crossRegionWriteLimit is null");
    }

    public CatalogNetworkMonitorProperties withTlsEnabled(boolean tlsEnabled)
    {
        return new CatalogNetworkMonitorProperties(
                catalogName,
                catalogId,
                tlsEnabled,
                crossRegionAllowed,
                crossRegionReadLimit,
                crossRegionWriteLimit);
    }

    public static CatalogNetworkMonitorProperties generateFrom(CrossRegionConfig crossRegionConfig, CatalogHandle catalogHandle)
    {
        Optional<DataSize> crossRegionReadLimit = crossRegionConfig.getAllowCrossRegionAccess()
                ? Optional.of(crossRegionConfig.getCrossRegionReadLimit())
                : Optional.empty();
        Optional<DataSize> crossRegionWriteLimit = crossRegionConfig.getAllowCrossRegionAccess()
                ? Optional.of(crossRegionConfig.getCrossRegionWriteLimit())
                : Optional.empty();
        return new CatalogNetworkMonitorProperties(
                catalogHandle.getCatalogName().toString(),
                catalogHandle.getId(),
                false,
                crossRegionConfig.getAllowCrossRegionAccess(),
                crossRegionReadLimit,
                crossRegionWriteLimit);
    }

    public static void addCatalogNetworkMonitorProperties(BiConsumer<String, String> propertiesConsumer, CatalogNetworkMonitorProperties properties)
    {
        propertiesConsumer.accept(CATALOG_NAME_PROPERTY_NAME, properties.catalogName());
        propertiesConsumer.accept(CATALOG_ID_PROPERTY_NAME, properties.catalogId());
        propertiesConsumer.accept(TLS_ENABLED_PROPERTY_NAME, String.valueOf(properties.tlsEnabled()));
        propertiesConsumer.accept(CROSS_REGION_ALLOWED_PROPERTY_NAME, String.valueOf(properties.crossRegionAllowed()));
        properties.crossRegionReadLimit().ifPresent(crossRegionReadLimit -> propertiesConsumer.accept(CROSS_REGION_READ_LIMIT_PROPERTY_NAME, crossRegionReadLimit.toString()));
        properties.crossRegionWriteLimit().ifPresent(crossRegionWriteLimit -> propertiesConsumer.accept(CROSS_REGION_WRITE_LIMIT_PROPERTY_NAME, crossRegionWriteLimit.toString()));
    }

    public static CatalogNetworkMonitorProperties getCatalogNetworkMonitorProperties(Function<String, Optional<String>> propertyProvider)
    {
        String catalogName = getRequiredProperty(propertyProvider, CATALOG_NAME_PROPERTY_NAME);
        String catalogId = getRequiredProperty(propertyProvider, CATALOG_ID_PROPERTY_NAME);
        boolean tlsEnabled = Boolean.parseBoolean(getRequiredProperty(propertyProvider, TLS_ENABLED_PROPERTY_NAME));
        boolean crossRegionAllowed = Boolean.parseBoolean(getRequiredProperty(propertyProvider, CROSS_REGION_ALLOWED_PROPERTY_NAME));
        Optional<DataSize> crossRegionReadLimit = getOptionalDataSizeProperty(propertyProvider, CROSS_REGION_READ_LIMIT_PROPERTY_NAME);
        Optional<DataSize> crossRegionWriteLimit = getOptionalDataSizeProperty(propertyProvider, CROSS_REGION_WRITE_LIMIT_PROPERTY_NAME);
        return new CatalogNetworkMonitorProperties(
                catalogName,
                catalogId,
                tlsEnabled,
                crossRegionAllowed,
                crossRegionReadLimit,
                crossRegionWriteLimit);
    }

    private static Optional<DataSize> getOptionalDataSizeProperty(Function<String, Optional<String>> propertyProvider, String propertyName)
    {
        return propertyProvider.apply(propertyName).map(DataSize::valueOf);
    }

    private static String getRequiredProperty(Function<String, Optional<String>> propertyProvider, String propertyName)
    {
        return propertyProvider.apply(propertyName)
                .orElseThrow(() -> new IllegalArgumentException("Missing required property: " + propertyName));
    }

    public static Optional<String> getOptionalProperty(Properties properties, String propertyName)
    {
        return Optional.ofNullable(properties.getProperty(propertyName));
    }
}
