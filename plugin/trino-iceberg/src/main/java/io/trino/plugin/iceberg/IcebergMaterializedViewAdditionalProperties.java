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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class IcebergMaterializedViewAdditionalProperties
{
    public static final String REFRESH_SCHEDULE = "refresh_schedule";
    public static final String STORAGE_SCHEMA = "storage_schema";

    private final List<PropertyMetadata<?>> materializedViewProperties;

    @Inject
    public IcebergMaterializedViewAdditionalProperties(IcebergConfig icebergConfig)
    {
        ImmutableList.Builder<PropertyMetadata<?>> materializedViewProperties = ImmutableList.builder();
        materializedViewProperties.addAll(ossProperties(icebergConfig));
        if (icebergConfig.isScheduledMaterializedViewRefreshEnabled()) {
            materializedViewProperties.add(stringProperty(
                    REFRESH_SCHEDULE,
                    "Cron schedule to use for refreshing the materialized view",
                    null,
                    false));
        }
        this.materializedViewProperties = materializedViewProperties.build();
    }

    private List<PropertyMetadata<?>> ossProperties(IcebergConfig icebergConfig)
    {
        return ImmutableList.of(stringProperty(
                STORAGE_SCHEMA,
                "Schema for creating materialized view storage table",
                icebergConfig.getMaterializedViewsStorageSchema().orElse(null),
                false));
    }

    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return materializedViewProperties;
    }

    public static Optional<String> getRefreshSchedule(Map<String, Object> materializedViewProperties)
    {
        return Optional.ofNullable((String) materializedViewProperties.get(REFRESH_SCHEDULE));
    }

    public static Optional<String> getStorageSchema(Map<String, Object> materializedViewProperties)
    {
        return Optional.ofNullable((String) materializedViewProperties.get(STORAGE_SCHEMA));
    }
}