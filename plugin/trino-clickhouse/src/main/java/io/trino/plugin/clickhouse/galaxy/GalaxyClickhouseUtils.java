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
package io.trino.plugin.clickhouse.galaxy;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.base.galaxy.CatalogNetworkMonitorProperties.CATALOG_ID_PROPERTY_NAME;
import static java.util.Objects.requireNonNull;

public class GalaxyClickhouseUtils
{
    private static final String KEY_VALUE_DELIMITER = "="; // Clickhouse driver expects comma separated additional properties with each property's key-value separated by an equals sign
    private static final Map<String, Properties> CATALOGS_PROPERTIES = new ConcurrentHashMap<>();

    private GalaxyClickhouseUtils() {}

    public static Properties deserializeProperties(Map<String, String> properties)
    {
        Properties parsedProperties = new Properties();
        String catalogId = requireNonNull(properties.get(CATALOG_ID_PROPERTY_NAME), "catalogId is null");
        Properties catalogProperties = requireNonNull(CATALOGS_PROPERTIES.get(catalogId), "catalogProperties is null");
        parsedProperties.putAll(catalogProperties);
        return parsedProperties;
    }

    public static String serializeProperties(String catalogId, Properties properties)
    {
        Properties catalogProperties = new Properties();
        catalogProperties.putAll(requireNonNull(properties, "properties is null"));
        CATALOGS_PROPERTIES.put(catalogId, catalogProperties);
        return "%s%s%s".formatted(CATALOG_ID_PROPERTY_NAME, KEY_VALUE_DELIMITER, catalogId);
    }
}
