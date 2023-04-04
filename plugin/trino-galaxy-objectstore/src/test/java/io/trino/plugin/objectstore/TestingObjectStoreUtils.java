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

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Verify.verify;

public final class TestingObjectStoreUtils
{
    private TestingObjectStoreUtils() {}

    public static Map<String, String> createObjectStoreProperties(
            TableType tableType,
            Map<String, String> locationSecurityClientConfig,
            Map<String, String> metastoreConfig,
            Map<String, String> hiveS3Config)
    {
        return createObjectStoreProperties(
                tableType,
                locationSecurityClientConfig,
                metastoreConfig,
                hiveS3Config,
                Map.of());
    }

    public static Map<String, String> createObjectStoreProperties(
            TableType tableType,
            Map<String, String> locationSecurityClientConfig,
            Map<String, String> metastoreConfig,
            Map<String, String> hiveS3Config,
            Map<String, String> extraObjectStoreProperties)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(createObjectStoreProperties(tableType));
        addObjectStoreProperties(properties, locationSecurityClientConfig);
        addCommonObjectStoreProperties(properties, locationSecurityClientConfig);
        addCommonObjectStoreProperties(properties, metastoreConfig);
        addCommonObjectStoreProperties(properties, hiveS3Config);

        verify(extraObjectStoreProperties.containsKey("HIVE__hive.metastore-cache-ttl") == extraObjectStoreProperties.containsKey("DELTA__hive.metastore-cache-ttl"), "Configure caching metastore for all or for none of underlying connectors");
        verify(extraObjectStoreProperties.containsKey("HIVE__hive.metastore-cache-ttl") == extraObjectStoreProperties.containsKey("HUDI__hive.metastore-cache-ttl"), "Configure caching metastore for all or for none of underlying connectors");
        if (!extraObjectStoreProperties.containsKey("HIVE__hive.metastore-cache-ttl")) {
            // Galaxy uses CachingHiveMetastore by default
            properties.put("HIVE__hive.metastore-cache-ttl", "2m");
            properties.put("DELTA__hive.metastore-cache-ttl", "2m");
            properties.put("HUDI__hive.metastore-cache-ttl", "2m");
        }
        properties.putAll(extraObjectStoreProperties);

        if (!extraObjectStoreProperties.containsKey("HIVE__hive.metastore")) {
            properties.put("HIVE__hive.metastore", "galaxy");
            properties.put("ICEBERG__iceberg.catalog.type", "GALAXY_METASTORE");
            properties.put("DELTA__hive.metastore", "galaxy");
            properties.put("HUDI__hive.metastore", "galaxy");
        }

        return properties.buildOrThrow();
    }

    private static void addObjectStoreProperties(ImmutableMap.Builder<String, String> target, Map<String, String> source)
    {
        source.forEach((key, value) -> target.put("OBJECTSTORE__" + key, value));
    }

    private static void addCommonObjectStoreProperties(ImmutableMap.Builder<String, String> target, Map<String, String> source)
    {
        source.forEach((key, value) -> {
            target.put("HIVE__" + key, value);
            target.put("ICEBERG__" + key, value);
            target.put("DELTA__" + key, value);
            target.put("HUDI__" + key, value);
        });
    }

    private static Map<String, String> createObjectStoreProperties(TableType tableType)
    {
        Map<String, String> properties = new HashMap<>();

        properties.put("OBJECTSTORE__object-store.table-type", tableType.toString());
        properties.put("HIVE__hive.allow-register-partition-procedure", "true");
        properties.put("HIVE__hive.non-managed-table-writes-enabled", "true");
        properties.put("ICEBERG__iceberg.register-table-procedure.enabled", "true");
        properties.put("DELTA__delta.register-table-procedure.enabled", "true");

        return properties;
    }
}
